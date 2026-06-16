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

	const routesWithData = routes.filter((r) => (r.total_headways ?? 0) > 0).length
	const meta = {
		title: 'CTA Bus Bunching Tracker — Live Chicago bus reliability',
		description: `Track bunching, gapping, and headway reliability across ${
			routesWithData || routes.length
		} CTA bus routes. See how evenly Chicago buses are spaced by route and time of day, updated continuously.`
	}

	return {
		routes,
		networkHourly,
		watermark,
		serviceId,
		bucket,
		...tableFilters,
		sortCol,
		sortDir,
		meta
	}
}
