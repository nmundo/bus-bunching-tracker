import type { PageServerLoad } from './$types'

export const load: PageServerLoad = async ({ fetch, params, url }) => {
	const serviceId = url.searchParams.get('service_id') ?? ''
	const bucket = url.searchParams.get('time_of_day_bucket') ?? 'AM_peak'
	const directionId = url.searchParams.get('direction_id') ?? ''
	const routeId = params.routeId

	const statsParams = new URLSearchParams()
	if (serviceId) statsParams.set('service_id', serviceId)
	if (bucket) statsParams.set('time_of_day_bucket', bucket)
	if (directionId !== '') statsParams.set('direction_id', directionId)

	const segmentsParams = new URLSearchParams()
	if (serviceId) segmentsParams.set('service_id', serviceId)
	if (bucket) segmentsParams.set('time_of_day_bucket', bucket)
	if (directionId !== '') segmentsParams.set('direction_id', directionId)

	const [statsRes, segmentsRes] = await Promise.all([
		fetch(`/api/routes/${routeId}/stats?${statsParams.toString()}`),
		fetch(`/api/routes/${routeId}/segments?${segmentsParams.toString()}`)
	])

	const stats = statsRes.ok ? await statsRes.json() : null
	const segments = segmentsRes.ok ? await segmentsRes.json() : null

	return {
		routeId,
		stats,
		segments,
		serviceId,
		bucket,
		directionId
	}
}
