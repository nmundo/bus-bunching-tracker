import { appendServiceFilter } from '$server/serviceFilter'

type BuildStatsInput = {
	routeId: string
	serviceId: string | null
	bucket: string | null
	directionId?: number | null
}

type BuildHourlyInput = {
	routeId: string
	serviceId: string | null
}

export type SegmentRow = {
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

export type SegmentFeatureProperties = {
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

export const _buildSummaryQuery = ({ routeId, serviceId, bucket, directionId }: BuildStatsInput) => {
	const filters: string[] = ['rbs.route_id = $1']
	const paramsList: unknown[] = [routeId]

	appendServiceFilter({ serviceId, serviceIdColumn: 'rbs.service_id', filters, params: paramsList })

	if (bucket) {
		paramsList.push(bucket)
		filters.push(`rbs.time_of_day_bucket = $${paramsList.length}`)
	}

	if (directionId !== null && directionId !== undefined) {
		paramsList.push(directionId)
		filters.push(`rbs.direction_id = $${paramsList.length}`)
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
      SUM(rbs.median_scheduled_headway * rbs.total_headways) / NULLIF(SUM(rbs.total_headways), 0) AS median_scheduled_headway,
      SUM(rbs.median_actual_headway * rbs.total_headways) / NULLIF(SUM(rbs.total_headways), 0) AS median_actual_headway
    FROM route_bunching_stats AS rbs
    WHERE ${filters.join(' AND ')}
  `

	return { sql, paramsList }
}

export const _buildHourlyBucketsQuery = ({ routeId, serviceId }: BuildHourlyInput) => {
	const filters: string[] = ['rhs.route_id = $1']
	const paramsList: unknown[] = [routeId]

	appendServiceFilter({ serviceId, serviceIdColumn: 'rhs.service_id', filters, params: paramsList })

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

type BuildSegmentsInput = {
	routeId: string
	serviceId: string | null
	bucket: string | null
	directionId?: number | null
}

export const _buildSegmentsQuery = ({ routeId, serviceId, bucket, directionId }: BuildSegmentsInput) => {
	const statsFilters: string[] = ['sbs.route_id = $1']
	const paramsList: unknown[] = [routeId]

	appendServiceFilter({ serviceId, serviceIdColumn: 'sbs.service_id', filters: statsFilters, params: paramsList })

	if (bucket) {
		paramsList.push(bucket)
		statsFilters.push(`sbs.time_of_day_bucket = $${paramsList.length}`)
	}

	const segmentFilters = ['s.route_id = $1']
	if (directionId !== null && directionId !== undefined) {
		paramsList.push(directionId)
		statsFilters.push(`sbs.direction_id = $${paramsList.length}`)
		segmentFilters.push(`s.direction_id = $${paramsList.length}`)
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

	return { sql, paramsList }
}

export const _buildSegmentFeatureCollection = (
	rows: SegmentRow[]
): GeoJSON.FeatureCollection<GeoJSON.LineString, SegmentFeatureProperties> => ({
	type: 'FeatureCollection',
	features: rows.map((row) => ({
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
			has_data: row.total_headways !== null && row.total_headways > 0
		}
	}))
})
