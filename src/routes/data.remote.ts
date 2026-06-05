import { query } from '$app/server'
import { query as dbQuery } from '$server/db'
import { appendServiceFilter } from '$server/serviceFilter'
import type { RouteStat, BucketStat } from '$lib/types/frontend'

type RoutesInput = { serviceId: string; bucket: string }
type HourlyInput = { serviceId: string }

export const getRoutes = query('unchecked', async ({ serviceId, bucket }: RoutesInput): Promise<RouteStat[]> => {
	const aggFilters: string[] = []
	const params: unknown[] = []

	appendServiceFilter({ serviceId: serviceId || null, serviceIdColumn: 'rbs.service_id', filters: aggFilters, params })
	if (bucket) {
		params.push(bucket)
		aggFilters.push(`rbs.time_of_day_bucket = $${params.length}`)
	}

	const wbFilters: string[] = []
	appendServiceFilter({ serviceId: serviceId || null, serviceIdColumn: 'wb2.service_id', filters: wbFilters, params })

	const aggWhere = aggFilters.length ? `WHERE ${aggFilters.join(' AND ')}` : ''
	const wbWhere = wbFilters.length
		? `WHERE ${wbFilters.join(' AND ')} AND wb2.bunching_rate IS NOT NULL`
		: `WHERE wb2.bunching_rate IS NOT NULL`

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

	const result = await dbQuery(sql, params)
	return result.rows as RouteStat[]
})

export const getNetworkHourly = query('unchecked', async ({ serviceId }: HourlyInput): Promise<BucketStat[]> => {
	const filters: string[] = []
	const params: unknown[] = []

	appendServiceFilter({ serviceId: serviceId || null, serviceIdColumn: 'rhs.service_id', filters, params })

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

	const result = await dbQuery(sql, params)
	return result.rows as BucketStat[]
})

export const getWatermark = query(async (): Promise<string | null> => {
	let watermark: string | null = null
	try {
		const r = await dbQuery<{ ts: string }>(
			`SELECT last_published_at AS ts FROM publish_meta WHERE id = 1`,
			[]
		)
		watermark = r.rows[0]?.ts ?? null
	} catch {
		/* not serving DB — try job_state */
	}
	if (!watermark) {
		try {
			const r = await dbQuery<{ ts: string }>(
				`SELECT watermark AS ts FROM job_state ORDER BY watermark DESC LIMIT 1`,
				[]
			)
			watermark = r.rows[0]?.ts ?? null
		} catch {
			/* no watermark available */
		}
	}
	return watermark
})
