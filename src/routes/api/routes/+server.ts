import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { query } from '$server/db'
import { appendServiceFilter } from '$server/serviceFilter'

export const GET: RequestHandler = async ({ url }) => {
	const serviceId = url.searchParams.get('service_id')
	const bucket = url.searchParams.get('time_of_day_bucket')

	// Filters for the main aggregation (service + bucket)
	const aggFilters: string[] = []
	const params: unknown[] = []

	appendServiceFilter({
		serviceId,
		serviceIdColumn: 'rbs.service_id',
		filters: aggFilters,
		params
	})
	if (bucket) {
		params.push(bucket)
		aggFilters.push(`rbs.time_of_day_bucket = $${params.length}`)
	}

	// Filters for worst_bucket sub-query (service only, no bucket — we want the worst across all periods)
	const wbFilters: string[] = []
	appendServiceFilter({
		serviceId,
		serviceIdColumn: 'wb2.service_id',
		filters: wbFilters,
		params
	})

	const aggWhere = aggFilters.length ? `WHERE ${aggFilters.join(' AND ')}` : ''
	const wbWhere = wbFilters.length ? `WHERE ${wbFilters.join(' AND ')} AND wb2.bunching_rate IS NOT NULL` : `WHERE wb2.bunching_rate IS NOT NULL`

	const sql = `
    WITH agg AS (
      SELECT
        rbs.route_id,
        SUM(rbs.total_headways)::int AS total_headways,
        SUM(rbs.bunched_headways)::int AS bunched_headways,
        SUM(rbs.super_bunched_headways)::int AS super_bunched_headways,
        SUM(COALESCE(rbs.gapped_headways, 0))::int AS gapped_headways,
        CASE
          WHEN SUM(rbs.total_headways) > 0
          THEN SUM(rbs.bunched_headways)::float / SUM(rbs.total_headways)
          ELSE NULL
        END AS bunching_rate,
        CASE
          WHEN SUM(rbs.total_headways) > 0
          THEN SUM(rbs.super_bunched_headways)::float / SUM(rbs.total_headways)
          ELSE NULL
        END AS super_bunching_rate,
        CASE
          WHEN SUM(rbs.total_headways) > 0
          THEN SUM(COALESCE(rbs.gapped_headways, 0))::float / SUM(rbs.total_headways)
          ELSE NULL
        END AS gapping_rate,
        AVG(rbs.avg_hw_ratio) AS avg_hw_ratio
      FROM route_bunching_stats rbs
      ${aggWhere}
      GROUP BY rbs.route_id
    ),
    worst_bucket AS (
      SELECT DISTINCT ON (wb2.route_id)
        wb2.route_id,
        wb2.time_of_day_bucket AS worst_bucket
      FROM route_bunching_stats wb2
      ${wbWhere}
      ORDER BY wb2.route_id, wb2.bunching_rate DESC
    )
    SELECT
      r.route_id,
      r.route_short_name,
      r.route_long_name,
      agg.total_headways,
      agg.bunched_headways,
      agg.bunching_rate,
      agg.super_bunching_rate,
      agg.gapping_rate,
      agg.avg_hw_ratio,
      wb.worst_bucket
    FROM gtfs_routes r
    LEFT JOIN agg ON agg.route_id = r.route_id
    LEFT JOIN worst_bucket wb ON wb.route_id = r.route_id
    ORDER BY agg.bunching_rate DESC NULLS LAST, r.route_short_name
  `

	const result = await query(sql, params)
	return json(result.rows)
}
