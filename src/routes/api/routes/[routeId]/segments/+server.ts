import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { query } from '$server/db'

export const GET: RequestHandler = async ({ params, url }) => {
	const routeId = params.routeId
	const serviceId = url.searchParams.get('service_id')
	const bucket = url.searchParams.get('time_of_day_bucket')

	const filters: string[] = ['s.route_id = $1']
	const paramsList: unknown[] = [routeId]

	if (serviceId) {
		paramsList.push(serviceId)
		filters.push(`sbs.service_id = $${paramsList.length}`)
	}
	if (bucket) {
		paramsList.push(bucket)
		filters.push(`sbs.time_of_day_bucket = $${paramsList.length}`)
	}

	const sql = `
    SELECT
      s.id AS segment_id,
      s.route_id,
      s.direction_id,
      s.from_stop_id,
      s.to_stop_id,
      fs.stop_name AS from_stop_name,
      ts.stop_name AS to_stop_name,
      sbs.bunching_rate,
      sbs.total_headways,
      sbs.time_of_day_bucket,
      ST_AsGeoJSON(s.geom)::json AS geometry
    FROM segments s
    LEFT JOIN segment_bunching_stats sbs ON sbs.segment_id = s.id
    LEFT JOIN gtfs_stops fs ON fs.stop_id = s.from_stop_id
    LEFT JOIN gtfs_stops ts ON ts.stop_id = s.to_stop_id
    WHERE ${filters.join(' AND ')}
  `

	const result = await query(sql, paramsList)

	const features = result.rows.map((row) => ({
		type: 'Feature',
		geometry: row.geometry,
		properties: {
			segment_id: row.segment_id,
			route_id: row.route_id,
			direction_id: row.direction_id,
			from_stop_id: row.from_stop_id,
			to_stop_id: row.to_stop_id,
			from_stop_name: row.from_stop_name,
			to_stop_name: row.to_stop_name,
			bunching_rate: row.bunching_rate,
			total_headways: row.total_headways,
			time_of_day_bucket: row.time_of_day_bucket
		}
	}))

	return json({
		type: 'FeatureCollection',
		features
	})
}
