import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { query } from '$server/db'
import { appendServiceFilter } from '$server/serviceFilter'

type BuildHourlyBucketsQueryInput = {
	routeId: string
	serviceId: string | null
}

type BuildSummaryQueryInput = {
	routeId: string
	serviceId: string | null
	bucket: string | null
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

export const _buildSummaryQuery = ({ routeId, serviceId, bucket }: BuildSummaryQueryInput) => {
	const filters: string[] = ['rbs.route_id = $1']
	const paramsList: unknown[] = [routeId]
	const scheduledFilters: string[] = ['he.route_id = $1', 'he.scheduled_headway_min IS NOT NULL']

	if (serviceId === 'weekday') {
		filters.push(`EXISTS (
      SELECT 1
      FROM gtfs_calendar gc
      WHERE gc.service_id = rbs.service_id
        AND (
          gc.monday = 1
          OR gc.tuesday = 1
          OR gc.wednesday = 1
          OR gc.thursday = 1
          OR gc.friday = 1
        )
    )`)
		scheduledFilters.push(`EXISTS (
      SELECT 1
      FROM gtfs_calendar gc
      WHERE gc.service_id = he.service_id
        AND (
          gc.monday = 1
          OR gc.tuesday = 1
          OR gc.wednesday = 1
          OR gc.thursday = 1
          OR gc.friday = 1
        )
    )`)
	} else if (serviceId === 'saturday') {
		filters.push(`EXISTS (
      SELECT 1
      FROM gtfs_calendar gc
      WHERE gc.service_id = rbs.service_id
        AND gc.saturday = 1
    )`)
		scheduledFilters.push(`EXISTS (
      SELECT 1
      FROM gtfs_calendar gc
      WHERE gc.service_id = he.service_id
        AND gc.saturday = 1
    )`)
	} else if (serviceId === 'sunday') {
		filters.push(`EXISTS (
      SELECT 1
      FROM gtfs_calendar gc
      WHERE gc.service_id = rbs.service_id
        AND gc.sunday = 1
    )`)
		scheduledFilters.push(`EXISTS (
      SELECT 1
      FROM gtfs_calendar gc
      WHERE gc.service_id = he.service_id
        AND gc.sunday = 1
    )`)
	} else if (serviceId) {
		paramsList.push(serviceId)
		filters.push(`rbs.service_id = $${paramsList.length}`)
		scheduledFilters.push(`he.service_id = $${paramsList.length}`)
	}

	if (bucket) {
		paramsList.push(bucket)
		filters.push(`rbs.time_of_day_bucket = $${paramsList.length}`)
		scheduledFilters.push(
			`coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time)) = $${paramsList.length}`
		)
	}

	const sql = `
    SELECT
      SUM(rbs.total_headways)::int AS total_headways,
      SUM(rbs.bunched_headways)::int AS bunched_headways,
      CASE
        WHEN SUM(rbs.total_headways) > 0
        THEN SUM(rbs.bunched_headways)::float / SUM(rbs.total_headways)
        ELSE NULL
      END AS bunching_rate,
      (
        SELECT percentile_cont(0.5) within group (order by he.scheduled_headway_min)
        FROM headways_enriched AS he
        WHERE ${scheduledFilters.join(' AND ')}
      ) AS median_scheduled_headway,
      AVG(rbs.median_actual_headway) AS median_actual_headway
    FROM route_bunching_stats AS rbs
    WHERE ${filters.join(' AND ')}
  `

	return { sql, paramsList }
}

export const GET: RequestHandler = async ({ params, url }) => {
	const routeId = params.routeId
	const serviceId = url.searchParams.get('service_id')
	const bucket = url.searchParams.get('time_of_day_bucket')

	const routeResult = await query(
		`SELECT route_id, route_short_name, route_long_name FROM gtfs_routes WHERE route_id = $1`,
		[routeId]
	)

	const { sql: summarySql, paramsList: summaryParams } = _buildSummaryQuery({
		routeId,
		serviceId,
		bucket
	})

	const { sql: bucketsSql, paramsList: bucketParams } = _buildHourlyBucketsQuery({
		routeId,
		serviceId
	})

	const [summaryResult, bucketsResult] = await Promise.all([
		query(summarySql, summaryParams),
		query(bucketsSql, bucketParams)
	])

	return json({
		route: routeResult.rows[0] ?? null,
		summary: summaryResult.rows[0] ?? null,
		buckets: bucketsResult.rows
	})
}
