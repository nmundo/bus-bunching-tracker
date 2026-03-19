import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { query } from '$server/db'
import { appendServiceFilter } from '$server/serviceFilter'

type BuildHourlyBucketsQueryInput = {
	routeId: string
	serviceId: string | null
}

export const _buildHourlyBucketsQuery = ({ routeId, serviceId }: BuildHourlyBucketsQueryInput) => {
	const filters: string[] = ['rhs.route_id = $1']
	const paramsList: unknown[] = [routeId]

	appendServiceFilter({
		serviceId,
		serviceIdColumn: 'rhs.service_id',
		filters,
		params: paramsList
	})

	const sql = `
    WITH hours AS (
      SELECT generate_series(0, 23)::int AS hour_of_day
    ),
    hourly AS (
      SELECT
        rhs.hour_of_day,
        SUM(rhs.total_headways)::int AS total_headways,
        SUM(rhs.bunched_headways)::int AS bunched_headways
      FROM route_hourly_bunching_stats AS rhs
      WHERE ${filters.join(' AND ')}
      GROUP BY 1
    )
    SELECT
      h.hour_of_day,
      COALESCE(hourly.total_headways, 0)::int AS total_headways,
      CASE
        WHEN COALESCE(hourly.total_headways, 0) > 0
        THEN hourly.bunched_headways::float / hourly.total_headways
        ELSE NULL
      END AS bunching_rate
    FROM hours AS h
    LEFT JOIN hourly ON hourly.hour_of_day = h.hour_of_day
    ORDER BY h.hour_of_day
  `

	return { sql, paramsList }
}

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

	const { sql: bucketsSql, paramsList: bucketParams } = _buildHourlyBucketsQuery({
		routeId,
		serviceId
	})

	const [summaryResult, bucketsResult] = await Promise.all([
		query(summarySql, baseParams),
		query(bucketsSql, bucketParams)
	])

	return json({
		route: routeResult.rows[0] ?? null,
		summary: summaryResult.rows[0] ?? null,
		buckets: bucketsResult.rows
	})
}
