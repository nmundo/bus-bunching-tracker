import { fileURLToPath } from 'node:url'
import { busTimeRequest } from './busTrackerClient'
import { query } from './db'

type RouteResponse = { routes: { rt: string; rtnm: string; rtclr?: string }[] }
type DirectionResponse = { directions: { dir: string }[] }
type StopResponse = { stops: { stpid: string; stpnm: string; lat: string; lon: string }[] }

type PatternPoint = {
	seq: string
	lat: string
	lon: string
	typ?: string
	stpid?: string
}

type Pattern = {
	pid: string
	rt: string
	rtdir: string
	pt: PatternPoint[]
}

type PatternResponse = { ptr: Pattern[] }

const upsertRoutes = async (routes: RouteResponse['routes']) => {
	if (!routes.length) return
	const values = routes.map((r) => [r.rt, r.rtnm, r.rtclr ?? null])
	const placeholders = values
		.map((row, index) => {
			const offset = index * 3
			return `($${offset + 1}, $${offset + 2}, $${offset + 3})`
		})
		.join(', ')

	await query(
		`insert into bt_routes (rt, rtnm, rtclr)
     values ${placeholders}
     on conflict (rt) do update set
       rtnm = excluded.rtnm,
       rtclr = excluded.rtclr`,
		values.flat()
	)
}

const upsertStops = async (rt: string, dir: string, stops: StopResponse['stops']) => {
	if (!stops.length) return
	const values = stops.map((s) => [s.stpid, s.stpnm, Number(s.lat), Number(s.lon), rt, dir])
	const placeholders = values
		.map((row, index) => {
			const offset = index * 6
			return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6})`
		})
		.join(', ')

	await query(
		`insert into bt_stops (stpid, stpnm, lat, lon, rt, dir)
     values ${placeholders}
     on conflict (stpid) do update set
       stpnm = excluded.stpnm,
       lat = excluded.lat,
       lon = excluded.lon,
       rt = excluded.rt,
       dir = excluded.dir`,
		values.flat()
	)
}

const upsertPatterns = async (patterns: Pattern[], knownStops: Set<string>) => {
	for (const pattern of patterns) {
		const coords = pattern.pt.map((pt) => `${pt.lon} ${pt.lat}`).join(', ')
		const wkt = `LINESTRING(${coords})`

		await query(
			`insert into bt_patterns (pid, rt, dir, geom)
       values ($1, $2, $3, ST_GeomFromText($4, 4326))
       on conflict (pid) do update set
         rt = excluded.rt,
         dir = excluded.dir,
         geom = excluded.geom`,
			[pattern.pid, pattern.rt, pattern.rtdir, wkt]
		)

		const stopPoints = pattern.pt.filter((pt) => pt.stpid && knownStops.has(pt.stpid))
		if (stopPoints.length) {
			const values = stopPoints.map((pt) => [pattern.pid, Number(pt.seq), pt.stpid])
			const placeholders = values
				.map((row, index) => {
					const offset = index * 3
					return `($${offset + 1}, $${offset + 2}, $${offset + 3})`
				})
				.join(', ')

			await query(
				`insert into bt_pattern_stops (pid, seq, stpid)
         values ${placeholders}
         on conflict do nothing`,
				values.flat()
			)
		}
	}
}

const loadKnownStops = async () => {
	const result = await query<{ stpid: string }>('select stpid from bt_stops')
	return new Set(result.rows.map((row) => row.stpid))
}

const refreshGeomsAndMappings = async () => {
	await query(`
    update bt_stops
    set geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326)
    where lon is not null and lat is not null
  `)

	await query(`
    insert into route_map (rt, gtfs_route_id)
    select bt.rt, gt.route_id
    from bt_routes bt
    join gtfs_routes gt
      on lower(bt.rt) = lower(gt.route_short_name)
    on conflict (rt) do update set
      gtfs_route_id = excluded.gtfs_route_id
  `)

	await query(`
    insert into stop_map (stpid, gtfs_stop_id)
    select bt.stpid, gt.stop_id
    from bt_stops bt
    join gtfs_stops gt on bt.stpid = gt.stop_id
    on conflict (stpid) do update set
      gtfs_stop_id = excluded.gtfs_stop_id
  `)

	await query(`
    insert into stop_map (stpid, gtfs_stop_id)
    select bt.stpid, nearest.stop_id
    from bt_stops bt
    left join stop_map sm on sm.stpid = bt.stpid
    join lateral (
      select stop_id
      from gtfs_stops
      where ST_DWithin(bt.geom::geography, gtfs_stops.geom::geography, 80)
      order by ST_Distance(bt.geom::geography, gtfs_stops.geom::geography)
      limit 1
    ) nearest on true
    where sm.stpid is null
    on conflict (stpid) do update set
      gtfs_stop_id = excluded.gtfs_stop_id
  `)
}

export const runSync = async () => {
	const routesPayload = await busTimeRequest<RouteResponse>('getroutes', {})
	const routes = routesPayload.routes ?? []
	await upsertRoutes(routes)

	for (const route of routes) {
		const dirPayload = await busTimeRequest<DirectionResponse>('getdirections', { rt: route.rt })
		const directions = dirPayload.directions ?? []

		for (const direction of directions) {
			const stopPayload = await busTimeRequest<StopResponse>('getstops', {
				rt: route.rt,
				dir: direction.dir
			})
			const stops = stopPayload.stops ?? []
			await upsertStops(route.rt, direction.dir, stops)
		}

		const patternPayload = await busTimeRequest<PatternResponse>('getpatterns', { rt: route.rt })
		const patterns = patternPayload.ptr ?? []
		if (patterns.length) {
			const knownStops = await loadKnownStops()
			await upsertPatterns(patterns, knownStops)
		}
	}

	await refreshGeomsAndMappings()
	console.log('Bus Tracker reference data synced')
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runSync().catch((error) => {
		console.error('Bus Tracker sync failed', error)
		process.exit(1)
	})
}
