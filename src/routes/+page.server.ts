import type { PageServerLoad } from './$types'
import { parseRouteTableFilters } from '$lib/ui/routeTableFilters'

export const load: PageServerLoad = async ({ fetch, url }) => {
	const serviceId = url.searchParams.get('service_id') ?? ''
	const bucket = url.searchParams.get('time_of_day_bucket') ?? 'AM_peak'
	const tableFilters = parseRouteTableFilters(url.searchParams)
	const params = new URLSearchParams()
	if (serviceId) params.set('service_id', serviceId)
	if (bucket) params.set('time_of_day_bucket', bucket)

	const hourlyParams = new URLSearchParams()
	if (serviceId) hourlyParams.set('service_id', serviceId)

	const [routesRes, hourlyRes, statusRes] = await Promise.all([
		fetch(`/api/routes?${params.toString()}`),
		fetch(`/api/network/hourly?${hourlyParams.toString()}`),
		fetch('/api/status')
	])

	const routes = routesRes.ok ? await routesRes.json() : []
	const networkHourly = hourlyRes.ok ? await hourlyRes.json() : []
	const status = statusRes.ok ? await statusRes.json() : {}

	return { routes, networkHourly, watermark: status.watermark ?? null, serviceId, bucket, ...tableFilters }
}
