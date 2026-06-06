import { query } from '$app/server'
import { query as dbQuery } from '$server/db'
import { appendServiceFilter } from '$server/serviceFilter'
import type { BucketStat } from '$lib/types/frontend'

type RouteInput = { routeId: string; serviceId: string; bucket: string; directionId: string }

type RouteSummary = {
	total_headways: number | null
	bunched_headways: number | null
	bunching_rate: number | null
	median_scheduled_headway: number | null
	median_actual_headway: number | null
}

type RouteInfo = {
	route_id: string
	route_short_name: string
	route_long_name: string | null
}

type StatsResult = {
	route: RouteInfo | null
	summary: RouteSummary | null
	buckets: BucketStat[]
}

type SegmentRow = {
	segment_id: string
	route_id: string
	direction_id: number | null
	from_stop_id: string | null
	to_stop_id: string | null
	from_stop_name: string | null
	to_stop_name: string | null
	bunching_rate: number | null
	total_headways: number | null
	bunched_headways: number | null
	time_of_day_bucket: string | null
	geometry: GeoJSON.Geometry
}

type SegmentFeatureProperties = {
	segment_id: string
	route_id: string
	direction_id: number | null
	from_stop_id: string | null
	to_stop_id: string | null
	from_stop_name: string | null
	to_stop_name: string | null
	bunching_rate: number | null
	total_headways: number | null
	bunched_headways: number | null
	time_of_day_bucket: string | null
	has_data: boolean
}

export const getRouteStats = query('unchecked', async ({ routeId, serviceId, bucket, directionId }: RouteInput): Promise<StatsResult> => {
	const directionIdInt = directionId !== '' ? parseInt(directionId, 10) : null

	const routeResult = await dbQuery<RouteInfo>(
		`SELECT route_id, route_short_name, route_long_name FROM gtfs_routes WHERE route_id = $1`,
		[routeId]
	)

	const summaryFilters: string[] = ['rbs.route_id = $1']
	const summaryParams: unknown[] = [routeId]
	appendServiceFilter({ serviceId: serviceId || null, serviceIdColumn: 'rbs.service_id', filters: summaryFilters, params: summaryParams })
	if (bucket) {
		summaryParams.push(bucket)
		summaryFilters.push(`rbs.time_of_day_bucket = $${summaryParams.length}`)
	}
	if (directionIdInt !== null) {
		summaryParams.push(directionIdInt)
		summaryFilters.push(`rbs.direction_id = $${summaryParams.length}`)
	}

	const summarySql = `
    SELECT
      SUM(rbs.total_headways)::int AS total_headways,
      SUM(rbs.bunched_headways)::int AS bunched_headways,
      CASE
        WHEN SUM(rbs.total_headways) > 0
        THEN SUM(rbs.bunched_headways)::float / SUM(rbs.total_headways)
        ELSE NULL
      END AS bunching_rate,
      AVG(rbs.median_scheduled_headway) AS median_scheduled_headway,
      AVG(rbs.median_actual_headway) AS median_actual_headway
    FROM route_bunching_stats AS rbs
    WHERE ${summaryFilters.join(' AND ')}
  `

	const hourlyFilters: string[] = ['rhs.route_id = $1']
	const hourlyParams: unknown[] = [routeId]
	appendServiceFilter({ serviceId: serviceId || null, serviceIdColumn: 'rhs.service_id', filters: hourlyFilters, params: hourlyParams })

	const bucketsSql = `
    WITH hours AS (
      SELECT generate_series(0, 23)::int AS hour_of_day
    ),
    hourly AS (
      SELECT
        rhs.hour_of_day,
        SUM(rhs.total_headways)::int AS total_headways,
        SUM(rhs.bunched_headways)::int AS bunched_headways
      FROM route_hourly_bunching_stats AS rhs
      WHERE ${hourlyFilters.join(' AND ')}
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

	const [summaryResult, bucketsResult] = await Promise.all([
		dbQuery<RouteSummary>(summarySql, summaryParams),
		dbQuery<BucketStat>(bucketsSql, hourlyParams)
	])

	return {
		route: routeResult.rows[0] ?? null,
		summary: summaryResult.rows[0] ?? null,
		buckets: bucketsResult.rows
	}
})

export const getRouteSegments = query('unchecked', async ({ routeId, serviceId, bucket, directionId }: RouteInput): Promise<GeoJSON.FeatureCollection<GeoJSON.LineString, SegmentFeatureProperties>> => {
	const directionIdInt = directionId !== '' ? parseInt(directionId, 10) : null

	const statsFilters: string[] = ['sbs.route_id = $1']
	const params: unknown[] = [routeId]
	appendServiceFilter({ serviceId: serviceId || null, serviceIdColumn: 'sbs.service_id', filters: statsFilters, params })
	if (bucket) {
		params.push(bucket)
		statsFilters.push(`sbs.time_of_day_bucket = $${params.length}`)
	}

	const segmentFilters = ['s.route_id = $1']
	if (directionIdInt !== null) {
		params.push(directionIdInt)
		statsFilters.push(`sbs.direction_id = $${params.length}`)
		segmentFilters.push(`s.direction_id = $${params.length}`)
	}

	const sql = `
    WITH filtered_stats AS (
      SELECT
        sbs.segment_id,
        SUM(sbs.total_headways)::int AS total_headways,
        SUM(sbs.bunched_headways)::int AS bunched_headways,
        CASE
          WHEN SUM(sbs.total_headways) > 0
          THEN SUM(sbs.bunched_headways)::float / SUM(sbs.total_headways)
          ELSE NULL
        END AS bunching_rate,
        CASE
          WHEN SUM(sbs.total_headways) > 0
          THEN MIN(sbs.time_of_day_bucket)
          ELSE NULL
        END AS time_of_day_bucket
      FROM segment_bunching_stats sbs
      WHERE ${statsFilters.join(' AND ')}
      GROUP BY sbs.segment_id
    )
    SELECT
      s.id AS segment_id,
      s.route_id,
      s.direction_id,
      s.from_stop_id,
      s.to_stop_id,
      from_stop.stop_name AS from_stop_name,
      to_stop.stop_name AS to_stop_name,
      filtered_stats.bunching_rate,
      filtered_stats.total_headways,
      filtered_stats.bunched_headways,
      filtered_stats.time_of_day_bucket,
      s.geometry AS geometry
    FROM segments s
    LEFT JOIN filtered_stats ON filtered_stats.segment_id = s.id
    LEFT JOIN gtfs_stops from_stop ON from_stop.stop_id = s.from_stop_id
    LEFT JOIN gtfs_stops to_stop ON to_stop.stop_id = s.to_stop_id
    WHERE ${segmentFilters.join(' AND ')}
    ORDER BY
      COALESCE(s.direction_id, -1),
      s.from_stop_id NULLS LAST,
      s.to_stop_id NULLS LAST,
      s.id
  `

	const result = await dbQuery<SegmentRow>(sql, params)

	return {
		type: 'FeatureCollection',
		features: result.rows.map((row) => {
			const hasData = row.total_headways !== null && row.total_headways > 0
			return {
				type: 'Feature',
				geometry: row.geometry as GeoJSON.LineString,
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
					bunched_headways: row.bunched_headways,
					time_of_day_bucket: row.time_of_day_bucket,
					has_data: hasData
				}
			}
		})
	}
})

export const getRouteDirections = query('unchecked', async ({ routeId }: { routeId: string }): Promise<Record<string, string>> => {
	try {
		const result = await dbQuery<{ direction_id: number; dir: string }>(
			`SELECT direction_id, dir FROM route_direction_labels WHERE route_id = $1`,
			[routeId]
		)
		const directions: Record<string, string> = {}
		for (const row of result.rows) {
			directions[String(row.direction_id)] = row.dir
		}
		return directions
	} catch {
		return {}
	}
})
