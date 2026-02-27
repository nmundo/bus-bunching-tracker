import { fileURLToPath } from 'node:url'
import { busTimeRequest } from './busTrackerClient'
import { query } from './db'
import { optionalEnv } from './env'

const DEFAULT_INTERVAL = 45
const DEFAULT_BATCH_SIZE = 6
const DEFAULT_LOW_TIER_MAX_STALENESS_SEC = 360
const DEFAULT_ACTIVITY_TTL_SEC = 360
const DEFAULT_ROUTE_REFRESH_SEC = 900
const MAX_BACKOFF_STEPS = 6
export const MAX_ROUTES_PER_REQUEST = 10

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

const insertVehicles = async (vehicles: Vehicle[], knownPatterns: Set<string>) => {
	if (!vehicles.length) return

	const values = vehicles.map((v) => [
		v.vid,
		v.rt,
		v.des,
		v.pid && knownPatterns.has(String(v.pid)) ? v.pid : null,
		Number(v.lat),
		Number(v.lon),
		v.pdist ? Number(v.pdist) : null,
		parseTimestamp(v.tmstmp),
		v.tatripid ?? null,
		v.tablockid ?? null
	])

	const placeholders = values
		.map((row, index) => {
			const offset = index * 10
			return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, ST_SetSRID(ST_MakePoint($${offset + 6}, $${offset + 5}), 4326), $${offset + 7}, (to_timestamp($${offset + 8}, 'YYYYMMDD HH24:MI:SS')::timestamp at time zone 'America/Chicago'), $${offset + 9}, $${offset + 10})`
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
	Number.isFinite(batchSize)
		? clamp(Math.floor(batchSize), 1, MAX_ROUTES_PER_REQUEST)
		: 1

export const getLowTierCycleMultiplier = (lowTierMaxStalenessSec: number, intervalSec: number) =>
	Math.max(
		1,
		Math.ceil(Math.max(1, Math.floor(lowTierMaxStalenessSec)) / Math.max(1, Math.floor(intervalSec)))
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

const getRouteUniverse = async (state: PollerState, config: PollerConfig, nowEpochSec: number) => {
	const envRoutes = parseEnvRoutes()
	if (envRoutes.length) {
		return unique(envRoutes).sort()
	}

	const shouldRefresh =
		!state.routeCache || nowEpochSec - state.routeCache.loadedAtEpochSec >= config.routeRefreshSec
	if (shouldRefresh) {
		const routes = await loadRoutesFromDb()
		state.routeCache = { routes, loadedAtEpochSec: nowEpochSec }
	}

	return state.routeCache?.routes ?? []
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

const runVehicleBatch = async (routes: string[]): Promise<BatchResult> => {
	try {
		const response = await busTimeRequest<VehiclesResponse>('getvehicles', { rt: routes.join(',') })
		const vehicles = response.vehicle ?? response.vehicles ?? []
		return {
			routes,
			vehicles,
			vehiclesCount: vehicles.length
		}
	} catch (error) {
		return {
			routes,
			vehicles: [],
			vehiclesCount: 0,
			error: toError(error)
		}
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

	const batches = chunkRoutes(plan.allRoutesThisCycle, config.batchSize)
	const batchResults: BatchResult[] = []
	for (const batch of batches) {
		const result = await runVehicleBatch(batch)
		if (result.error) {
			console.error(`getvehicles batch failed for routes ${batch.join(',')}`, result.error)
		}
		batchResults.push(result)
	}

	const failedBatches = batchResults.filter((result) => result.error)
	const successfulBatches = batchResults.filter((result) => !result.error)
	const vehicles = successfulBatches.flatMap((result) => result.vehicles)

	if (vehicles.length > 0) {
		const knownPatterns = await loadKnownPatterns()
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

	while (true) {
		try {
			await pollOnce(state, config)
			backoff = 0
			await sleep(config.intervalSec * 1000)
		} catch (error) {
			console.error('Poller failed', error)
			backoff = Math.min(backoff + 1, MAX_BACKOFF_STEPS)
			const wait = Math.pow(2, backoff) * 1000
			await sleep(wait)
		} finally {
			state.cycleIndex += 1
		}
	}
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runPoller().catch((error) => {
		console.error('Poller crashed', error)
		process.exit(1)
	})
}
