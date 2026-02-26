import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { query } from '$server/db'
import { appendServiceFilter } from '$server/serviceFilter'

export const GET: RequestHandler = async ({ params, url }) => {
	const routeId = params.routeId
	const serviceId = url.searchParams.get('service_id')

	const filters: string[] = ['rbs.route_id = $1']
	const baseParams: unknown[] = [routeId]
	appendServiceFilter({
		serviceId,
		serviceIdColumn: 'rbs.service_id',
		filters,
		params: baseParams
	})
	const whereSql = `WHERE ${filters.join(' AND ')}`

	const routeResult = await query(
		`SELECT route_id, route_short_name, route_long_name FROM gtfs_routes WHERE route_id = $1`,
		[routeId]
	)

	const summarySql = `
    SELECT
      SUM(total_headways)::int AS total_headways,
      SUM(bunched_headways)::int AS bunched_headways,
      CASE
        WHEN SUM(total_headways) > 0
        THEN SUM(bunched_headways)::float / SUM(total_headways)
        ELSE NULL
      END AS bunching_rate,
      AVG(avg_hw_ratio) AS avg_hw_ratio,
      AVG(median_actual_headway) AS median_actual_headway
    FROM route_bunching_stats
    AS rbs
    ${whereSql}
  `

	const bucketsSql = `
    SELECT time_of_day_bucket, AVG(bunching_rate) AS bunching_rate
    FROM route_bunching_stats AS rbs
    ${whereSql}
    GROUP BY time_of_day_bucket
    ORDER BY time_of_day_bucket
  `

	const [summaryResult, bucketsResult] = await Promise.all([
		query(summarySql, baseParams),
		query(bucketsSql, baseParams)
	])

	return json({
		route: routeResult.rows[0] ?? null,
		summary: summaryResult.rows[0] ?? null,
		buckets: bucketsResult.rows
	})
}
