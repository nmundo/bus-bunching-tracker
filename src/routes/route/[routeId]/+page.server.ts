import type { PageServerLoad } from './$types'
import { getRouteStats, getRouteSegments, getRouteDirections } from './data.remote'

export const load: PageServerLoad = async ({ params, url }) => {
	const serviceId = url.searchParams.get('service_id') ?? ''
	const bucket = url.searchParams.get('time_of_day_bucket') ?? 'AM_peak'
	const directionId = url.searchParams.get('direction_id') ?? ''
	const routeId = params.routeId

	const [stats, segments, directions] = await Promise.all([
		getRouteStats({ routeId, serviceId, bucket, directionId }),
		getRouteSegments({ routeId, serviceId, bucket, directionId }),
		getRouteDirections({ routeId })
	])

	const route = stats?.route
	const shortName = route?.route_short_name ?? routeId
	const longName = route?.route_long_name
	const rate = stats?.summary?.bunching_rate
	const ratePct = rate !== null && rate !== undefined ? `${(rate * 100).toFixed(1)}%` : null
	const meta = {
		title: `Route ${shortName}${longName ? ` (${longName})` : ''} — CTA Bus Bunching Tracker`,
		description: `Bunching, gapping, and headway reliability for CTA Route ${shortName}${
			longName ? ` ${longName}` : ''
		}.${ratePct ? ` Current bunching rate ${ratePct}.` : ''} See how evenly buses are spaced by time of day.`,
		// Don't index detail pages for routes that have no data behind them.
		noindex: !route
	}

	return {
		routeId,
		stats,
		segments,
		directions,
		serviceId,
		bucket,
		directionId,
		meta
	}
}
