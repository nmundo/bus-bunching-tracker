import type { PageServerLoad } from './$types'

export const load: PageServerLoad = async ({ fetch, url }) => {
	const serviceId = url.searchParams.get('service_id') ?? ''
	const bucket = url.searchParams.get('time_of_day_bucket') ?? 'AM_peak'
	const params = new URLSearchParams()
	if (serviceId) params.set('service_id', serviceId)
	if (bucket) params.set('time_of_day_bucket', bucket)

	const res = await fetch(`/api/routes?${params.toString()}`)
	const routes = res.ok ? await res.json() : []

	return { routes, serviceId, bucket }
}
