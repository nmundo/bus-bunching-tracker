import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { query } from '$server/db'
import { appendServiceFilter } from '$server/serviceFilter'

export const GET: RequestHandler = async ({ url }) => {
	const serviceId = url.searchParams.get('service_id')

	const filters: string[] = []
	const params: unknown[] = []

	appendServiceFilter({
		serviceId,
		serviceIdColumn: 'rhs.service_id',
		filters,
		params
	})

	const whereSql = filters.length ? `WHERE ${filters.join(' AND ')}` : ''

	const sql = `
    WITH hours AS (
      SELECT generate_series(0, 23)::int AS hour_of_day
    ),
    agg AS (
      SELECT
        rhs.hour_of_day,
        SUM(rhs.total_headways)::int AS total_headways,
        SUM(rhs.bunched_headways)::int AS bunched_headways
      FROM route_hourly_bunching_stats rhs
      ${whereSql}
      GROUP BY rhs.hour_of_day
    )
    SELECT
      h.hour_of_day,
      COALESCE(agg.total_headways, 0)::int AS total_headways,
      CASE
        WHEN COALESCE(agg.total_headways, 0) > 0
        THEN agg.bunched_headways::float / agg.total_headways
        ELSE NULL
      END AS bunching_rate
    FROM hours h
    LEFT JOIN agg ON agg.hour_of_day = h.hour_of_day
    ORDER BY h.hour_of_day
  `

	const result = await query(sql, params)
	return json(result.rows)
}
