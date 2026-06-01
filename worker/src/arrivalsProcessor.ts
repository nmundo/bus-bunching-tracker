import { fileURLToPath } from 'node:url'
import { query } from './db'

const FEET_PER_METER = 3.28084
const STOP_ARRIVAL_INSERT_BATCH_SIZE = 1000

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

type StopArrivalInsert = {
	route_id: string
	direction_id: number | null
	stop_id: string
	vid: string
	rt: string
	pid: string
	arrival_time: Date
	pdist_feet: number
}

const directionToId = (dir: string | null) => {
	if (!dir) return null
	const lower = dir.toLowerCase()
	if (lower.includes('north') || lower.includes('west')) return 0
	if (lower.includes('south') || lower.includes('east')) return 1
	return null
}

const loadPatternStops = async () => {
	const result = await query<{
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
      (ST_LineLocatePoint(bp.geom, bs.geom) * ST_Length(bp.geom::geography) * ${FEET_PER_METER}) as distance_feet
    from bt_pattern_stops ps
    join bt_patterns bp on bp.pid = ps.pid
    join bt_stops bs on bs.stpid = ps.stpid
    left join stop_map sm on sm.stpid = ps.stpid
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

const loadRouteMap = async () => {
	const result = await query<{ rt: string; gtfs_route_id: string }>(
		'select rt, gtfs_route_id from route_map'
	)
	const map = new Map<string, string>()
	for (const row of result.rows) {
		map.set(row.rt, row.gtfs_route_id)
	}
	return map
}

const loadPatternDirections = async () => {
	const result = await query<{ pid: string; dir: string | null }>(
		'select pid, dir from bt_patterns'
	)
	const map = new Map<string, string | null>()
	for (const row of result.rows) {
		map.set(row.pid, row.dir)
	}
	return map
}

const getWatermark = async () => {
	const result = await query<{ watermark: Date | null }>(
		`select watermark from job_state where id = 'arrivals_processor'`
	)
	return result.rows[0]?.watermark ?? null
}

const setWatermark = async (watermark: Date) => {
	await query(
		`insert into job_state (id, watermark)
     values ('arrivals_processor', $1)
     on conflict (id) do update set watermark = excluded.watermark`,
		[watermark]
	)
}

const chunk = <T>(items: T[], size: number) => {
	const chunks: T[][] = []
	for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size))
	return chunks
}

export const _buildStopArrivalsInsertSql = (rowCount: number) => {
	const columns = [
		'route_id',
		'direction_id',
		'stop_id',
		'vid',
		'rt',
		'pid',
		'arrival_time',
		'pdist_feet'
	]
	const values = Array.from({ length: rowCount }, (_, rowIndex) => {
		const placeholders = columns.map(
			(_, columnIndex) => `$${rowIndex * columns.length + columnIndex + 1}`
		)
		return `(${placeholders.join(', ')})`
	})

	return `
    insert into stop_arrivals (${columns.join(', ')})
    values ${values.join(', ')}
  `
}

export const _flattenStopArrivalRows = (rows: StopArrivalInsert[]) =>
	rows.flatMap((row) => [
		row.route_id,
		row.direction_id,
		row.stop_id,
		row.vid,
		row.rt,
		row.pid,
		row.arrival_time,
		row.pdist_feet
	])

const insertStopArrivals = async (rows: StopArrivalInsert[]) => {
	for (const batch of chunk(rows, STOP_ARRIVAL_INSERT_BATCH_SIZE)) {
		await query(_buildStopArrivalsInsertSql(batch.length), _flattenStopArrivalRows(batch))
	}
}

export const runArrivals = async () => {
	const patternStops = await loadPatternStops()
	const routeMap = await loadRouteMap()
	const patternDirs = await loadPatternDirections()
	const watermark = await getWatermark()

	const result = await query<{
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
     order by tmstmp asc`,
		[watermark]
	)

	const states = new Map<string, BusState>()
	const arrivalsToInsert: StopArrivalInsert[] = []
	let lastTimestamp = watermark

	for (const row of result.rows) {
		if (!row.pid || row.pdist_feet === null) continue
		const stops = patternStops.get(row.pid)
		if (!stops || stops.length === 0) continue

		const stateKey = `${row.vid}-${row.pid}`
		const state = states.get(stateKey) ?? {
			vid: row.vid,
			rt: row.rt,
			pid: row.pid,
			lastPdist: row.pdist_feet,
			lastTimestamp: row.tmstmp,
			lastStopIndex: -1
		}

		const routeId = routeMap.get(row.rt)
		const directionId = directionToId(patternDirs.get(row.pid) ?? null)
		let nextIndex = state.lastStopIndex + 1
		while (nextIndex < stops.length && row.pdist_feet >= stops[nextIndex].distance_feet) {
			const nextStop = stops[nextIndex]
			if (routeId && nextStop.gtfs_stop_id) {
				arrivalsToInsert.push({
					route_id: routeId,
					direction_id: directionId,
					stop_id: nextStop.gtfs_stop_id,
					vid: row.vid,
					rt: row.rt,
					pid: row.pid,
					arrival_time: row.tmstmp,
					pdist_feet: row.pdist_feet
				})
			}

			state.lastStopIndex = nextIndex
			nextIndex += 1
		}

		state.lastPdist = row.pdist_feet
		state.lastTimestamp = row.tmstmp
		states.set(stateKey, state)
		lastTimestamp = row.tmstmp
	}

	await insertStopArrivals(arrivalsToInsert)

	if (lastTimestamp) {
		await setWatermark(lastTimestamp)
	}
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runArrivals()
		.then(() => {
			console.log('Arrivals processed')
			process.exit(0)
		})
		.catch((error) => {
			console.error('Arrivals processing failed', error)
			process.exit(1)
		})
}
