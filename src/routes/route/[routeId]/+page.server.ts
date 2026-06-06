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

	return {
		routeId,
		stats,
		segments,
		directions,
		serviceId,
		bucket,
		directionId
	}
}
