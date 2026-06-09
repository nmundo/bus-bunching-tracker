import { fileURLToPath } from 'node:url'
import { busTimeRequestTolerant, CtaBusTrackerError } from './busTrackerClient'
import type { CtaBusTrackerErrorDetail } from './busTrackerClient'
import { query, closePool } from './db'
import { optionalEnv } from './env'

const DEFAULT_INTERVAL = 45
const DEFAULT_BATCH_SIZE = 6
const DEFAULT_LOW_TIER_MAX_STALENESS_SEC = 360
const DEFAULT_ACTIVITY_TTL_SEC = 360
const DEFAULT_ROUTE_REFRESH_SEC = 900
const MAX_BACKOFF_STEPS = 6
const MAX_CONSECUTIVE_FAILURES = 10
export const MAX_ROUTES_PER_REQUEST = 10

// Patterns only change during the nightly sync; 1 hour TTL is more than enough.
const DEFAULT_PATTERNS_REFRESH_SEC = 3600

// Schedule-aware filtering: refresh the set of "currently scheduled" routes
// every 5 min. Windows below are wide enough that a route stays in the set
// while any of its trips is in progress.
const DEFAULT_SCHEDULE_REFRESH_SEC = 300
const DEFAULT_SCHEDULE_LOOKBACK_MIN = 90
const DEFAULT_SCHEDULE_LOOKAHEAD_MIN = 30

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const toError = (error: unknown) => (error instanceof Error ? error : new Error(String(error)))

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const parsePositiveInt = (value: string, fallback: number) => {
	const parsed = Number(value)
	if (!Number.isFinite(parsed) || parsed <= 0) return fallback
	return Math.floor(parsed)
}

const unique = <T>(list: T[]) => Array.from(new Set(list))

const chunkRoutes = (list: string[], size: number) => {
	const safeSize = clamp(size, 1, MAX_ROUTES_PER_REQUEST)
	const result: string[][] = []
	for (let i = 0; i < list.length; i += safeSize) {
		result.push(list.slice(i, i + safeSize))
	}
	return result
}

export type Vehicle = {
	vid: string
	rt: string
	des: string
	pid?: string
	lat: string
	lon: string
	pdist?: string
	tmstmp: string
	tatripid?: string
	tablockid?: string
}

type VehiclesResponse = { vehicle?: Vehicle[]; vehicles?: Vehicle[] }

export type RouteActivityState = {
	lastSeenEpochSec: number
}

export type PollPlan = {
	fastRoutes: string[]
	slowBucketRoutes: string[]
	allRoutesThisCycle: string[]
}

export type BatchResult = {
	routes: string[]
	vehicles: Vehicle[]
	vehiclesCount: number
	error?: Error
}

type PollerConfig = {
	intervalSec: number
	batchSize: number
	lowTierMaxStalenessSec: number
	activityTtlSec: number
	routeRefreshSec: number
	lowTierCycleMultiplier: number
}

type PollerState = {
	routeCache: {
		routes: string[]
		loadedAtEpochSec: number
	} | null
	knownPatternsCache: {
		patterns: Set<string>
		loadedAtEpochSec: number
	} | null
	scheduledRoutesCache: {
		routes: Set<string>
		loadedAtEpochSec: number
	} | null
	routeActivity: Map<string, RouteActivityState>
	cycleIndex: number
}

const parseTimestamp = (value: string) => {
	const trimmed = value?.trim()
	if (!trimmed) return value
	const parts = trimmed.split(' ')
	if (parts.length !== 2) return trimmed
	const time = parts[1]
	const colonCount = (time.match(/:/g) ?? []).length
	if (colonCount === 1) return `${parts[0]} ${time}:00`
	return trimmed
}

const loadKnownPatterns = async () => {
	const result = await query<{ pid: string }>('select pid from bt_patterns')
	return new Set(result.rows.map((row) => row.pid))
}

// Return the cached known-patterns set, refreshing if older than
// DEFAULT_PATTERNS_REFRESH_SEC.  Patterns only change during the nightly sync
// so a 1-hour TTL eliminates ~80 full-table reads per hour with no correctness
// trade-off (an unknown pid is simply stored as null, same as before).
const getKnownPatterns = async (state: PollerState, nowEpochSec: number): Promise<Set<string>> => {
	const stale =
		!state.knownPatternsCache ||
		nowEpochSec - state.knownPatternsCache.loadedAtEpochSec >= DEFAULT_PATTERNS_REFRESH_SEC

	if (stale) {
		const patterns = await loadKnownPatterns()
		state.knownPatternsCache = { patterns, loadedAtEpochSec: nowEpochSec }
	}

	return state.knownPatternsCache!.patterns
}

const insertVehicles = async (vehicles: Vehicle[], knownPatterns: Set<string>) => {
	if (!vehicles.length) return

	// Column order (1-based per row):
	//  $1=vid  $2=rt  $3=des  $4=pid  $5=lat  $6=lon
	//  $7=pdist_feet  $8=tmstmp  $9=tatripid  $10=tablockid
	// ST_MakePoint takes (longitude, latitude) → ($6, $5)
	const values = vehicles.map((v) => [
		v.vid, // 1  vid
		v.rt, // 2  rt
		v.des, // 3  des
		v.pid && knownPatterns.has(String(v.pid)) ? v.pid : null, // 4  pid
		Number(v.lat), // 5  lat
		Number(v.lon), // 6  lon
		v.pdist ? Number(v.pdist) : null, // 7  pdist_feet
		parseTimestamp(v.tmstmp), // 8  tmstmp
		v.tatripid ?? null, // 9  tatripid
		v.tablockid ?? null // 10 tablockid
	])

	const placeholders = values
		.map((_, index) => {
			const o = index * 10
			// Explicit mapping comment kept so any future column reorder is obvious.
			// lon=$o+6, lat=$o+5 → ST_MakePoint(lon, lat) is correct PostGIS order.
			return `($${o + 1},$${o + 2},$${o + 3},$${o + 4},$${o + 5},$${o + 6},ST_SetSRID(ST_MakePoint($${o + 6},$${o + 5}),4326),$${o + 7},(to_timestamp($${o + 8},'YYYYMMDD HH24:MI:SS')::timestamp at time zone 'America/Chicago'),$${o + 9},$${o + 10})`
		})
		.join(', ')

	const sql = `
    insert into bus_positions (
      vid, rt, des, pid, lat, lon, geom, pdist_feet, tmstmp, tatripid, tablockid
    )
    values ${placeholders}
  `

	await query(sql, values.flat())
}

const parseEnvRoutes = () =>
	optionalEnv('CTA_BUS_TRACKER_ROUTES', '')
		.split(',')
		.map((r) => r.trim())
		.filter(Boolean)

const loadRoutesFromDb = async () => {
	const result = await query<{ rt: string }>('select rt from bt_routes')
	return unique(result.rows.map((row) => row.rt).filter(Boolean)).sort()
}

export const clampBatchSize = (batchSize: number) =>
	Number.isFinite(batchSize) ? clamp(Math.floor(batchSize), 1, MAX_ROUTES_PER_REQUEST) : 1

export const getLowTierCycleMultiplier = (lowTierMaxStalenessSec: number, intervalSec: number) =>
	Math.max(
		1,
		Math.ceil(
			Math.max(1, Math.floor(lowTierMaxStalenessSec)) / Math.max(1, Math.floor(intervalSec))
		)
	)

const readPollerConfig = (): PollerConfig => {
	const intervalSec = parsePositiveInt(
		optionalEnv('CTA_BUS_TRACKER_POLL_INTERVAL_SEC', String(DEFAULT_INTERVAL)),
		DEFAULT_INTERVAL
	)

	const batchSize = clampBatchSize(
		parsePositiveInt(
			optionalEnv('CTA_BUS_TRACKER_BATCH_SIZE', String(DEFAULT_BATCH_SIZE)),
			DEFAULT_BATCH_SIZE
		)
	)

	const lowTierMaxStalenessSec = parsePositiveInt(
		optionalEnv(
			'CTA_BUS_TRACKER_LOW_TIER_MAX_STALENESS_SEC',
			String(DEFAULT_LOW_TIER_MAX_STALENESS_SEC)
		),
		DEFAULT_LOW_TIER_MAX_STALENESS_SEC
	)
	const activityTtlSec = parsePositiveInt(
		optionalEnv('CTA_BUS_TRACKER_ACTIVITY_TTL_SEC', String(DEFAULT_ACTIVITY_TTL_SEC)),
		DEFAULT_ACTIVITY_TTL_SEC
	)
	const routeRefreshSec = parsePositiveInt(
		optionalEnv('CTA_BUS_TRACKER_ROUTE_REFRESH_SEC', String(DEFAULT_ROUTE_REFRESH_SEC)),
		DEFAULT_ROUTE_REFRESH_SEC
	)
	const lowTierCycleMultiplier = getLowTierCycleMultiplier(lowTierMaxStalenessSec, intervalSec)

	if (activityTtlSec < lowTierMaxStalenessSec) {
		console.warn(
			`CTA_BUS_TRACKER_ACTIVITY_TTL_SEC (${activityTtlSec}) is lower than CTA_BUS_TRACKER_LOW_TIER_MAX_STALENESS_SEC (${lowTierMaxStalenessSec}); routes may drop from fast tier earlier than staleness threshold`
		)
	}

	return {
		intervalSec,
		batchSize,
		lowTierMaxStalenessSec,
		activityTtlSec,
		routeRefreshSec,
		lowTierCycleMultiplier
	}
}

// Routes whose GTFS schedule places at least one trip's first stop
// in [now - lookback, now + lookahead] under an active service day in
// America/Chicago. Buses for any other route would always return
// "No data found" — skipping them eliminates wasted CTA calls and the
// per-row errors that previously crashed the poller overnight.
const loadScheduledRoutes = async (): Promise<Set<string>> => {
	const lookbackMin = parsePositiveInt(
		optionalEnv('CTA_BUS_TRACKER_SCHEDULE_LOOKBACK_MIN', String(DEFAULT_SCHEDULE_LOOKBACK_MIN)),
		DEFAULT_SCHEDULE_LOOKBACK_MIN
	)
	const lookaheadMin = parsePositiveInt(
		optionalEnv('CTA_BUS_TRACKER_SCHEDULE_LOOKAHEAD_MIN', String(DEFAULT_SCHEDULE_LOOKAHEAD_MIN)),
		DEFAULT_SCHEDULE_LOOKAHEAD_MIN
	)

	// `bucket = 'today'` matches trips on today's service day; `'yesterday'`
	// catches trips that started yesterday but have departure_time > 24h
	// (GTFS overnight encoding).
	const sql = `
		with cfg as (
			select
				(now() at time zone 'America/Chicago')::date as service_today,
				((now() at time zone 'America/Chicago')::date - 1) as service_yesterday,
				((now() at time zone 'America/Chicago')::time)::interval as now_today,
				((now() at time zone 'America/Chicago')::time)::interval + interval '24 hours' as now_overnight,
				extract(dow from (now() at time zone 'America/Chicago'))::int as today_dow,
				extract(dow from ((now() at time zone 'America/Chicago') - interval '1 day'))::int as yesterday_dow,
				make_interval(mins => $1::int) as lookback,
				make_interval(mins => $2::int) as lookahead
		),
		active_services as (
			select c.service_id, 'today'::text as bucket
			from gtfs_calendar c, cfg
			where cfg.service_today between c.start_date and c.end_date
				and (case cfg.today_dow
					when 0 then c.sunday when 1 then c.monday when 2 then c.tuesday
					when 3 then c.wednesday when 4 then c.thursday when 5 then c.friday
					when 6 then c.saturday end) = 1
				and not exists (
					select 1 from gtfs_calendar_dates cd
					where cd.service_id = c.service_id
						and cd.date = cfg.service_today
						and cd.exception_type = 2
				)
			union
			select cd.service_id, 'today'::text
			from gtfs_calendar_dates cd, cfg
			where cd.date = cfg.service_today and cd.exception_type = 1
			union
			select c.service_id, 'yesterday'::text
			from gtfs_calendar c, cfg
			where cfg.service_yesterday between c.start_date and c.end_date
				and (case cfg.yesterday_dow
					when 0 then c.sunday when 1 then c.monday when 2 then c.tuesday
					when 3 then c.wednesday when 4 then c.thursday when 5 then c.friday
					when 6 then c.saturday end) = 1
				and not exists (
					select 1 from gtfs_calendar_dates cd
					where cd.service_id = c.service_id
						and cd.date = cfg.service_yesterday
						and cd.exception_type = 2
				)
			union
			select cd.service_id, 'yesterday'::text
			from gtfs_calendar_dates cd, cfg
			where cd.date = cfg.service_yesterday and cd.exception_type = 1
		),
		active_route_ids as (
			select distinct t.route_id
			from gtfs_trips t
			join active_services s on s.service_id = t.service_id
			join gtfs_stop_times st on st.trip_id = t.trip_id and st.stop_sequence = 1
			cross join cfg
			where (s.bucket = 'today'
					and st.departure_time between cfg.now_today - cfg.lookback and cfg.now_today + cfg.lookahead)
				or (s.bucket = 'yesterday'
					and st.departure_time between cfg.now_overnight - cfg.lookback and cfg.now_overnight + cfg.lookahead)
			union
			select distinct t.route_id
			from gtfs_trips t
			join active_services s on s.service_id = t.service_id
			join gtfs_frequencies f on f.trip_id = t.trip_id
			cross join cfg
			where (s.bucket = 'today'
					and cfg.now_today - cfg.lookback <= f.end_time
					and cfg.now_today + cfg.lookahead >= f.start_time)
				or (s.bucket = 'yesterday'
					and cfg.now_overnight - cfg.lookback <= f.end_time
					and cfg.now_overnight + cfg.lookahead >= f.start_time)
		)
		select distinct rm.rt
		from route_map rm
		join active_route_ids a on a.route_id = rm.gtfs_route_id
	`
	const result = await query<{ rt: string }>(sql, [lookbackMin, lookaheadMin])
	return new Set(result.rows.map((row) => row.rt).filter(Boolean))
}

const getScheduledRoutes = async (
	state: PollerState,
	nowEpochSec: number
): Promise<Set<string> | null> => {
	if (optionalEnv('CTA_BUS_TRACKER_SCHEDULE_FILTER', '1') === '0') return null

	const stale =
		!state.scheduledRoutesCache ||
		nowEpochSec - state.scheduledRoutesCache.loadedAtEpochSec >= DEFAULT_SCHEDULE_REFRESH_SEC

	if (stale) {
		try {
			const routes = await loadScheduledRoutes()
			state.scheduledRoutesCache = { routes, loadedAtEpochSec: nowEpochSec }
		} catch (error) {
			// If the schedule query fails (e.g. missing GTFS data), don't kill
			// the poller — just fall back to the unfiltered universe.
			console.warn(
				'Scheduled-routes query failed; falling back to unfiltered route universe',
				toError(error).message
			)
			return null
		}
	}

	return state.scheduledRoutesCache?.routes ?? null
}

const getRouteUniverse = async (state: PollerState, config: PollerConfig, nowEpochSec: number) => {
	const envRoutes = parseEnvRoutes()
	const baseRoutes = envRoutes.length
		? unique(envRoutes).sort()
		: await (async () => {
				const shouldRefresh =
					!state.routeCache ||
					nowEpochSec - state.routeCache.loadedAtEpochSec >= config.routeRefreshSec
				if (shouldRefresh) {
					const routes = await loadRoutesFromDb()
					state.routeCache = { routes, loadedAtEpochSec: nowEpochSec }
				}
				return state.routeCache?.routes ?? []
			})()

	const scheduled = await getScheduledRoutes(state, nowEpochSec)
	if (!scheduled) return baseRoutes
	const filtered = baseRoutes.filter((rt) => scheduled.has(rt))
	// If the filter would zero us out (e.g. GTFS data not yet imported, or
	// every route happens to be off-schedule right now), trust the schedule —
	// returning [] makes the cycle a no-op, which is the desired overnight
	// behavior.
	return filtered
}

const pruneRouteActivity = (
	routeActivity: Map<string, RouteActivityState>,
	nowEpochSec: number,
	activityTtlSec: number
) => {
	for (const [routeId, state] of routeActivity) {
		if (nowEpochSec - state.lastSeenEpochSec > activityTtlSec) {
			routeActivity.delete(routeId)
		}
	}
}

const hashRoute = (value: string) => {
	let hash = 2166136261
	for (let i = 0; i < value.length; i += 1) {
		hash ^= value.charCodeAt(i)
		hash = Math.imul(hash, 16777619)
	}
	return hash >>> 0
}

export const stableRouteBucket = (routeId: string, bucketCount: number) => {
	const safeBucketCount = Math.max(1, Math.floor(bucketCount))
	return hashRoute(routeId) % safeBucketCount
}

export const buildPollPlan = ({
	routes,
	routeActivity,
	nowEpochSec,
	lowTierMaxStalenessSec,
	lowTierCycleMultiplier,
	cycleIndex
}: {
	routes: string[]
	routeActivity: Map<string, RouteActivityState>
	nowEpochSec: number
	lowTierMaxStalenessSec: number
	lowTierCycleMultiplier: number
	cycleIndex: number
}): PollPlan => {
	const fastRoutes: string[] = []
	const slowRoutes: string[] = []

	for (const routeId of routes) {
		const state = routeActivity.get(routeId)
		if (state && nowEpochSec - state.lastSeenEpochSec <= lowTierMaxStalenessSec) {
			fastRoutes.push(routeId)
		} else {
			slowRoutes.push(routeId)
		}
	}

	const selectedSlowBucket = cycleIndex % lowTierCycleMultiplier
	const slowBucketRoutes = slowRoutes.filter(
		(routeId) => stableRouteBucket(routeId, lowTierCycleMultiplier) === selectedSlowBucket
	)
	const allRoutesThisCycle = unique([...fastRoutes, ...slowBucketRoutes]).sort()

	return {
		fastRoutes: fastRoutes.sort(),
		slowBucketRoutes: slowBucketRoutes.sort(),
		allRoutesThisCycle
	}
}

// The CTA API uses several phrasings for "this route has no service right
// now" — all are valid empty results, not real failures, and must not count
// against the consecutive-failure budget or the poller crashes every night.
const NO_DATA_PHRASES = ['no vehicle', 'no data found', 'no service scheduled']

export const isNoVehicleMessage = (message: string) => {
	const lower = message.toLowerCase()
	return NO_DATA_PHRASES.some((phrase) => lower.includes(phrase))
}

export const isNoVehicleError = (err: CtaBusTrackerError) => isNoVehicleMessage(err.message)

const isAllNoVehicleErrors = (errors: CtaBusTrackerErrorDetail[]) =>
	errors.length > 0 &&
	errors.every((e) => typeof e.msg === 'string' && isNoVehicleMessage(e.msg))

const runVehicleBatch = async (routes: string[]): Promise<BatchResult> => {
	try {
		const { response, errors } = await busTimeRequestTolerant<VehiclesResponse>('getvehicles', {
			rt: routes.join(',')
		})
		const vehicles = response.vehicle ?? response.vehicles ?? []

		// If we got vehicles, the batch is a success even if some routes also
		// reported "no data" — those are per-route status messages, not batch
		// failures. If we got no vehicles but every error is a no-data error,
		// treat it as an empty (not failed) batch.
		if (vehicles.length > 0 || errors.length === 0 || isAllNoVehicleErrors(errors)) {
			return { routes, vehicles, vehiclesCount: vehicles.length }
		}

		const errorMessage = errors
			.map((e) => e.msg)
			.filter((m): m is string => typeof m === 'string' && m.trim().length > 0)
			.join('; ')
		return {
			routes,
			vehicles: [],
			vehiclesCount: 0,
			error: new CtaBusTrackerError({
				message: errorMessage
					? `CTA Bus Tracker API error: ${errorMessage}`
					: 'CTA Bus Tracker API returned an error response',
				endpoint: 'getvehicles',
				details: errors
			})
		}
	} catch (error) {
		if (error instanceof CtaBusTrackerError && isNoVehicleError(error)) {
			return { routes, vehicles: [], vehiclesCount: 0 }
		}
		return { routes, vehicles: [], vehiclesCount: 0, error: toError(error) }
	}
}

type PollCycleMetrics = {
	routesUniverseCount: number
	fastRoutesCount: number
	slowRoutesPolledCount: number
	callsMade: number
	failedBatches: number
	successfulBatches: number
	vehiclesReceived: number
}

const logPollCycle = (metrics: PollCycleMetrics, config: PollerConfig) => {
	const callsPerHour = (metrics.callsMade * 3600) / config.intervalSec
	const vehiclesPerCall = metrics.callsMade === 0 ? 0 : metrics.vehiclesReceived / metrics.callsMade
	console.info('Poll cycle summary', {
		routesUniverse: metrics.routesUniverseCount,
		fastRoutes: metrics.fastRoutesCount,
		slowRoutesPolled: metrics.slowRoutesPolledCount,
		callsMade: metrics.callsMade,
		failedBatches: metrics.failedBatches,
		vehiclesReceived: metrics.vehiclesReceived,
		callsPerHour: Number(callsPerHour.toFixed(2)),
		vehiclesPerCall: Number(vehiclesPerCall.toFixed(2))
	})
}

export const createPollerState = (): PollerState => ({
	routeCache: null,
	knownPatternsCache: null,
	scheduledRoutesCache: null,
	routeActivity: new Map<string, RouteActivityState>(),
	cycleIndex: 0
})

export const pollOnce = async (state: PollerState, config: PollerConfig) => {
	const nowEpochSec = Math.floor(Date.now() / 1000)
	pruneRouteActivity(state.routeActivity, nowEpochSec, config.activityTtlSec)

	const routes = await getRouteUniverse(state, config, nowEpochSec)
	const plan = buildPollPlan({
		routes,
		routeActivity: state.routeActivity,
		nowEpochSec,
		lowTierMaxStalenessSec: config.lowTierMaxStalenessSec,
		lowTierCycleMultiplier: config.lowTierCycleMultiplier,
		cycleIndex: state.cycleIndex
	})

	if (plan.allRoutesThisCycle.length === 0) {
		// Nothing scheduled — no API calls, no failure.
		const metrics: PollCycleMetrics = {
			routesUniverseCount: routes.length,
			fastRoutesCount: 0,
			slowRoutesPolledCount: 0,
			callsMade: 0,
			failedBatches: 0,
			successfulBatches: 0,
			vehiclesReceived: 0
		}
		logPollCycle(metrics, config)
		return metrics
	}

	const batches = chunkRoutes(plan.allRoutesThisCycle, config.batchSize)
	const batchResults: BatchResult[] = []
	for (const batch of batches) {
		const result = await runVehicleBatch(batch)
		if (result.error) {
			const status =
				result.error instanceof CtaBusTrackerError ? ` (HTTP ${result.error.status})` : ''
			console.error(
				`getvehicles batch failed for routes ${batch.join(',')}${status}: ${result.error.message}`
			)
		}
		batchResults.push(result)
	}

	const failedBatches = batchResults.filter((result) => result.error)
	const successfulBatches = batchResults.filter((result) => !result.error)
	const vehicles = successfulBatches.flatMap((result) => result.vehicles)

	if (vehicles.length > 0) {
		const knownPatterns = await getKnownPatterns(state, nowEpochSec)
		await insertVehicles(vehicles, knownPatterns)
		for (const vehicle of vehicles) {
			state.routeActivity.set(vehicle.rt, { lastSeenEpochSec: nowEpochSec })
		}
	}

	const metrics: PollCycleMetrics = {
		routesUniverseCount: routes.length,
		fastRoutesCount: plan.fastRoutes.length,
		slowRoutesPolledCount: plan.slowBucketRoutes.length,
		callsMade: batchResults.length,
		failedBatches: failedBatches.length,
		successfulBatches: successfulBatches.length,
		vehiclesReceived: vehicles.length
	}

	logPollCycle(metrics, config)

	if (batchResults.length > 0 && successfulBatches.length === 0) {
		throw new Error(
			`All getvehicles batches failed for cycle ${state.cycleIndex} (${failedBatches.length}/${batchResults.length})`
		)
	}

	return metrics
}

export const runPoller = async () => {
	const config = readPollerConfig()
	const state = createPollerState()
	let backoff = 0
	let consecutiveFailures = 0

	while (true) {
		try {
			await pollOnce(state, config)
			consecutiveFailures = 0
			backoff = 0
			await sleep(config.intervalSec * 1000)
		} catch (error) {
			consecutiveFailures += 1
			console.error(`Poller failed (${consecutiveFailures}/${MAX_CONSECUTIVE_FAILURES})`, error)
			if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
				console.error(
					`Poller exceeded ${MAX_CONSECUTIVE_FAILURES} consecutive failures — exiting so the process supervisor can restart`
				)
				process.exit(1)
			}
			backoff = Math.min(backoff + 1, MAX_BACKOFF_STEPS)
			const wait = Math.pow(2, backoff) * 1000
			await sleep(wait)
		} finally {
			state.cycleIndex += 1
		}
	}
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runPoller().catch(async (error) => {
		console.error('Poller crashed', error)
		await closePool()
		process.exit(1)
	})
}
