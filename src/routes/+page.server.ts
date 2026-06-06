import type { PageServerLoad } from './$types'
import { parseRouteTableFilters, parseSortParams } from '$lib/ui/routeTableFilters'
import { getRoutes, getNetworkHourly, getWatermark } from './data.remote'

export const load: PageServerLoad = async ({ url }) => {
	const serviceId = url.searchParams.get('service_id') ?? ''
	const bucket = url.searchParams.get('time_of_day_bucket') ?? 'AM_peak'
	const tableFilters = parseRouteTableFilters(url.searchParams)
	const { sortCol, sortDir } = parseSortParams(url.searchParams)

	const [routes, networkHourly, watermark] = await Promise.all([
		getRoutes({ serviceId, bucket }),
		getNetworkHourly({ serviceId }),
		getWatermark()
	])

	return { routes, networkHourly, watermark, serviceId, bucket, ...tableFilters, sortCol, sortDir }
}
