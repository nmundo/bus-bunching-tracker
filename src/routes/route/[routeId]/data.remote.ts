import { query } from '$app/server'
import { query as dbQuery } from '$server/db'
import type { BucketStat } from '$lib/types/frontend'
import {
	_buildSummaryQuery,
	_buildHourlyBucketsQuery,
	_buildDailyTrendQuery,
	_buildSegmentsQuery,
	_buildSegmentFeatureCollection,
	type SegmentRow,
	type SegmentFeatureProperties,
	type DailyTrendRow
} from './queryBuilders'

type RouteInput = { routeId: string; serviceId: string; bucket: string; directionId: string }

type RouteSummary = {
	total_headways: number | null
	bunched_headways: number | null
	bunching_rate: number | null
	mean_scheduled_headway: number | null
	mean_actual_headway: number | null
	excess_wait_min: number | null
	headway_cv: number | null
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
	dailyTrend: DailyTrendRow[]
}

export const getRouteStats = query('unchecked', async ({ routeId, serviceId, bucket, directionId }: RouteInput): Promise<StatsResult> => {
	const directionIdInt = directionId !== '' ? parseInt(directionId, 10) : null

	const routeResult = await dbQuery<RouteInfo>(
		`SELECT route_id, route_short_name, route_long_name FROM gtfs_routes WHERE route_id = $1`,
		[routeId]
	)

	const { sql: summarySql, paramsList: summaryParams } = _buildSummaryQuery({
		routeId,
		serviceId: serviceId || null,
		bucket: bucket || null,
		directionId: directionIdInt
	})

	const { sql: bucketsSql, paramsList: hourlyParams } = _buildHourlyBucketsQuery({
		routeId,
		serviceId: serviceId || null
	})

	const { sql: trendSql, paramsList: trendParams } = _buildDailyTrendQuery({
		routeId,
		serviceId: serviceId || null
	})

	const [summaryResult, bucketsResult, trendResult] = await Promise.all([
		dbQuery<RouteSummary>(summarySql, summaryParams),
		dbQuery<BucketStat>(bucketsSql, hourlyParams),
		dbQuery<DailyTrendRow>(trendSql, trendParams)
	])

	return {
		route: routeResult.rows[0] ?? null,
		summary: summaryResult.rows[0] ?? null,
		buckets: bucketsResult.rows,
		dailyTrend: trendResult.rows
	}
})

export const getRouteSegments = query('unchecked', async ({ routeId, serviceId, bucket, directionId }: RouteInput): Promise<GeoJSON.FeatureCollection<GeoJSON.LineString, SegmentFeatureProperties>> => {
	const directionIdInt = directionId !== '' ? parseInt(directionId, 10) : null

	const { sql, paramsList } = _buildSegmentsQuery({
		routeId,
		serviceId: serviceId || null,
		bucket: bucket || null,
		directionId: directionIdInt
	})

	const result = await dbQuery<SegmentRow>(sql, paramsList)
	return _buildSegmentFeatureCollection(result.rows)
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
