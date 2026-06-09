import { fileURLToPath } from 'node:url'
import { getPool, closePool } from './db'

const FEET_PER_METER = 3.28084

// Cap the number of bus_positions rows consumed per run.  Under normal
// operation ~3 500 rows arrive per 5-minute window; 50 000 is a safe ceiling
// that prevents OOM after a long outage while still draining a backlog quickly.
const ARRIVALS_BATCH_LIMIT = 50_000

type PatternStop = {
	seq: number
	stpid: string
	gtfs_stop_id: string | null
	distance_feet: number
}

type BusState = {
	vid: string
	rt: string
	pid: string
	lastPdist: number
	lastTimestamp: Date
	lastStopIndex: number
}

const directionToId = (dir: string | null) => {
	if (!dir) return null
	const lower = dir.toLowerCase()
	if (lower.includes('north') || lower.includes('west')) return 0
	if (lower.includes('south') || lower.includes('east')) return 1
	return null
}

// Estimate the moment a vehicle actually passed a stop by linearly interpolating
// between the two surrounding pdist samples, rather than snapping to the poll
// timestamp (which carries up to a full poll interval of error).  Falls back to
// the current poll timestamp when there is no usable prior sample (a vehicle
// seen for the first time this run, the new Date(0) seed sentinel, or no forward
// movement between samples).
export const interpolateCrossingTime = ({
	stopDistance,
	prevPdist,
	prevTimestamp,
	currPdist,
	currTimestamp
}: {
	stopDistance: number
	prevPdist: number
	prevTimestamp: Date
	currPdist: number
	currTimestamp: Date
}): Date => {
	const prevMs = prevTimestamp.getTime()
	if (prevMs <= 0 || currPdist <= prevPdist) {
		return currTimestamp
	}
	const rawFraction = (stopDistance - prevPdist) / (currPdist - prevPdist)
	const fraction = Math.min(1, Math.max(0, rawFraction))
	return new Date(prevMs + fraction * (currTimestamp.getTime() - prevMs))
}

// Highest stop index whose distance is at or below pdist — i.e. the last stop the
// vehicle has already passed. A full scan (rather than a binary search) tolerates
// non-monotonic distances from GPS or pattern-data quirks. Returns -1 only when
// the vehicle sits before the very first stop.
export const stopIndexForPdist = (stops: PatternStop[], pdist: number): number => {
	let index = -1
	for (let i = 0; i < stops.length; i++) {
		if (stops[i].distance_feet <= pdist) index = i
	}
	return index
}

// Use the precomputed distance_feet column instead of running
// ST_LineLocatePoint / ST_Length on every 5-minute cycle.  Stops whose
// distance_feet is still null (added since the last sync) are excluded; they
// will be picked up after the next nightly sync populates the column.
const loadPatternStops = async (client: import('pg').PoolClient) => {
	const result = await client.query<{
		pid: string
		seq: number
		stpid: string
		gtfs_stop_id: string | null
		distance_feet: number
	}>(`
    select
      ps.pid,
      ps.seq,
      ps.stpid,
      sm.gtfs_stop_id,
      ps.distance_feet
    from bt_pattern_stops ps
    left join stop_map sm on sm.stpid = ps.stpid
    where ps.distance_feet is not null
    order by ps.pid, ps.seq
  `)

	const map = new Map<string, PatternStop[]>()
	for (const row of result.rows) {
		if (!map.has(row.pid)) map.set(row.pid, [])
		map.get(row.pid)?.push({
			seq: row.seq,
			stpid: row.stpid,
			gtfs_stop_id: row.gtfs_stop_id,
			distance_feet: row.distance_feet
		})
	}
	return map
}

const loadRouteMap = async (client: import('pg').PoolClient) => {
	const result = await client.query<{ rt: string; gtfs_route_id: string }>(
		'select rt, gtfs_route_id from route_map'
	)
	const map = new Map<string, string>()
	for (const row of result.rows) {
		map.set(row.rt, row.gtfs_route_id)
	}
	return map
}

const loadPatternDirections = async (client: import('pg').PoolClient) => {
	const result = await client.query<{ pid: string; dir: string | null }>(
		'select pid, dir from bt_patterns'
	)
	const map = new Map<string, string | null>()
	for (const row of result.rows) {
		map.set(row.pid, row.dir)
	}
	return map
}

// Seed in-memory vehicle state from recent stop_arrivals so that when the job
// restarts (or resumes after a gap) it does not re-fire arrivals for stops the
// vehicle already passed in previous runs.
//
// Without this, every new run resets lastStopIndex to -1, causing the while
// loop to re-insert arrivals for all stops from 0 to the current position.
// The unique index on stop_arrivals provides a second line of defence, but
// seeding prevents the spurious inserts in the first place.
//
// The seed window is relative to the most recent arrival already recorded, NOT
// wall-clock now(). In live operation max(arrival_time) ≈ now() so this is
// unchanged. But during a cold backfill/replay of historical bus_positions, the
// data is hours-to-weeks old: anchoring on now() would find zero seeds, so at
// every batch boundary each vehicle would be treated as first-seen, re-anchored
// at its current position, and lose the stops it crossed since the previous
// batch — silently under-counting arrivals and inflating headways. Keying the
// window off the latest recorded arrival keeps consecutive replay batches
// continuous. We also seed lastTimestamp from the actual arrival time (not the
// epoch sentinel) so the first crossing after a boundary interpolates instead of
// collapsing every crossed stop onto one timestamp.
const loadVehicleStates = async (
	client: import('pg').PoolClient,
	patternStops: Map<string, PatternStop[]>
): Promise<Map<string, BusState>> => {
	const result = await client.query<{
		vid: string
		pid: string
		rt: string
		last_pdist: number
		last_time: Date
	}>(`
    select distinct on (vid, pid, rt)
      vid, pid, rt, pdist_feet as last_pdist, arrival_time as last_time
    from stop_arrivals
    where arrival_time > (select coalesce(max(arrival_time), now()) from stop_arrivals) - interval '24 hours'
      and pdist_feet is not null
    order by vid, pid, rt, arrival_time desc
  `)

	const states = new Map<string, BusState>()
	for (const row of result.rows) {
		const stops = patternStops.get(row.pid)
		if (!stops || stops.length === 0) continue

		states.set(`${row.vid}-${row.pid}`, {
			vid: row.vid,
			rt: row.rt,
			pid: row.pid,
			lastPdist: row.last_pdist,
			lastTimestamp: new Date(row.last_time),
			lastStopIndex: stopIndexForPdist(stops, row.last_pdist)
		})
	}
	return states
}

const getWatermark = async (client: import('pg').PoolClient) => {
	const result = await client.query<{ watermark: Date | null }>(
		`select watermark from job_state where id = 'arrivals_processor'`
	)
	return result.rows[0]?.watermark ?? null
}

const setWatermark = async (client: import('pg').PoolClient, watermark: Date) => {
	await client.query(
		`insert into job_state (id, watermark)
     values ('arrivals_processor', $1)
     on conflict (id) do update set watermark = excluded.watermark`,
		[watermark]
	)
}

type StopArrivalRow = {
	route_id: string
	direction_id: number | null
	stop_id: string
	vid: string
	rt: string
	pid: string
	arrival_time: Date
	pdist_feet: number
}

const ARRIVAL_COLUMNS = 8
const ARRIVAL_INSERT_BATCH_ROWS = 500

export const _buildStopArrivalsInsertSql = (rowCount: number): string => {
	const placeholders = Array.from({ length: rowCount }, (_, i) => {
		const o = i * ARRIVAL_COLUMNS
		return `($${o + 1}, $${o + 2}, $${o + 3}, $${o + 4}, $${o + 5}, $${o + 6}, $${o + 7}, $${o + 8})`
	}).join(', ')
	return `insert into stop_arrivals
         (route_id, direction_id, stop_id, vid, rt, pid, arrival_time, pdist_feet)
       values ${placeholders}
       on conflict (route_id, stop_id, vid, arrival_time) do nothing`
}

export const _flattenStopArrivalRows = (rows: StopArrivalRow[]): unknown[] =>
	rows.flatMap((r) => [r.route_id, r.direction_id, r.stop_id, r.vid, r.rt, r.pid, r.arrival_time, r.pdist_feet])

const insertArrivals = async (client: import('pg').PoolClient, rows: StopArrivalRow[]) => {
	if (rows.length === 0) return

	for (let offset = 0; offset < rows.length; offset += ARRIVAL_INSERT_BATCH_ROWS) {
		const batch = rows.slice(offset, offset + ARRIVAL_INSERT_BATCH_ROWS)
		await client.query(_buildStopArrivalsInsertSql(batch.length), _flattenStopArrivalRows(batch))
	}
}

export const runArrivals = async () => {
	const client = await getPool().connect()
	try {
		await client.query('begin')

		const watermark = await getWatermark(client)

		const result = await client.query<{
			id: number
			vid: string
			rt: string
			pid: string | null
			pdist_feet: number | null
			tmstmp: Date
		}>(
			`select id, vid, rt, pid, pdist_feet, tmstmp
       from bus_positions
       where tmstmp is not null
         and ($1::timestamptz is null or tmstmp > $1)
       order by tmstmp asc
       limit ${ARRIVALS_BATCH_LIMIT}`,
			[watermark]
		)

		if (result.rows.length === 0) {
			await client.query('rollback')
			return
		}

		// Capture the ceiling of this batch before processing so the watermark
		// represents "everything up to this point has been processed" regardless
		// of what arrives in stop_arrivals during our run.
		const newWatermark = result.rows[result.rows.length - 1].tmstmp

		const patternStops = await loadPatternStops(client)
		const routeMap = await loadRouteMap(client)
		const patternDirs = await loadPatternDirections(client)

		// Seed vehicle state from recent arrivals to avoid re-firing stops that
		// were already recorded in previous runs.
		const states = await loadVehicleStates(client, patternStops)

		const arrivalRows: StopArrivalRow[] = []

		for (const row of result.rows) {
			if (!row.pid || row.pdist_feet === null) continue
			const stops = patternStops.get(row.pid)
			if (!stops || stops.length === 0) continue

			const stateKey = `${row.vid}-${row.pid}`
			// First sighting this run with no seeded history: anchor at the vehicle's
			// current position so the while loop below emits nothing this poll. Without
			// this (lastStopIndex: -1) it would backfill every stop from index 0 up to
			// the current pdist, all stamped with this poll's timestamp — the multi-stop
			// timestamp-collision bug that floods stop_arrivals with phantom rows and
			// collapses headways. Real arrivals are emitted once the vehicle advances on
			// a subsequent poll. loadVehicleStates handles vehicles with recent history;
			// this covers first sightings, returning vehicles, and pid changes.
			const state = states.get(stateKey) ?? {
				vid: row.vid,
				rt: row.rt,
				pid: row.pid,
				lastPdist: row.pdist_feet,
				lastTimestamp: row.tmstmp,
				lastStopIndex: stopIndexForPdist(stops, row.pdist_feet)
			}

			const routeId = routeMap.get(row.rt)
			const directionId = directionToId(patternDirs.get(row.pid) ?? null)

			// Snapshot the prior sample before we overwrite it below; it anchors the
			// interpolation of every stop crossed during this poll interval.
			const prevPdist = state.lastPdist
			const prevTimestamp = state.lastTimestamp
			let nextIndex = state.lastStopIndex + 1

			while (nextIndex < stops.length && row.pdist_feet >= stops[nextIndex].distance_feet) {
				const nextStop = stops[nextIndex]
				if (routeId && nextStop.gtfs_stop_id) {
					arrivalRows.push({
						route_id: routeId,
						direction_id: directionId,
						stop_id: nextStop.gtfs_stop_id,
						vid: row.vid,
						rt: row.rt,
						pid: row.pid,
						arrival_time: interpolateCrossingTime({
							stopDistance: nextStop.distance_feet,
							prevPdist,
							prevTimestamp,
							currPdist: row.pdist_feet,
							currTimestamp: row.tmstmp
						}),
						pdist_feet: row.pdist_feet
					})
				}
				state.lastStopIndex = nextIndex
				nextIndex += 1
			}

			state.lastPdist = row.pdist_feet
			state.lastTimestamp = row.tmstmp
			states.set(stateKey, state)
		}

		await insertArrivals(client, arrivalRows)
		await setWatermark(client, newWatermark)

		await client.query('commit')
	} catch (err) {
		await client.query('rollback')
		throw err
	} finally {
		client.release()
	}
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runArrivals()
		.then(async () => {
			console.log('Arrivals processed')
			await closePool()
			process.exit(0)
		})
		.catch(async (error) => {
			console.error('Arrivals processing failed', error)
			await closePool()
			process.exit(1)
		})
}
