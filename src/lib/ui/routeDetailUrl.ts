export type RouteDetailFilters = {
	serviceId: string
	bucket: string
}

export const withRouteDetailFilterParams = (
	searchParams: URLSearchParams,
	filters: RouteDetailFilters
): URLSearchParams => {
	const next = new URLSearchParams(searchParams)

	if (filters.serviceId) {
		next.set('service_id', filters.serviceId)
	} else {
		next.delete('service_id')
	}

	if (filters.bucket) {
		next.set('time_of_day_bucket', filters.bucket)
	} else {
		next.delete('time_of_day_bucket')
	}

	return next
}

export const buildRouteDetailHref = (
	routeId: string,
	filters: RouteDetailFilters
): string => {
	const params = withRouteDetailFilterParams(new URLSearchParams(), filters)
	const query = params.toString()

	return query ? `/route/${routeId}?${query}` : `/route/${routeId}`
}
