import { requireEnv } from './env'

const BASE_URL = 'https://www.ctabustracker.com/bustime/api/v3'

export type BusTimeResponse<T> = {
	'bustime-response': T
}

export type CtaBusTrackerErrorDetail = {
	msg?: string
	[key: string]: unknown
}

type BusTimeResponseBody<T> = T & { error?: CtaBusTrackerErrorDetail[] }

export class CtaBusTrackerError extends Error {
	endpoint: string
	details: CtaBusTrackerErrorDetail[]
	status: number | null

	constructor(options: {
		message: string
		endpoint: string
		details?: CtaBusTrackerErrorDetail[]
		status?: number | null
	}) {
		super(options.message)
		this.name = 'CtaBusTrackerError'
		this.endpoint = options.endpoint
		this.details = options.details ?? []
		this.status = options.status ?? null
	}
}

const getApiKey = () => requireEnv('CTA_BUS_TRACKER_API_KEY')

const buildUrl = (endpoint: string, params: Record<string, string>) => {
	const url = new URL(`${BASE_URL}/${endpoint}`)
	url.searchParams.set('key', getApiKey())
	url.searchParams.set('format', 'json')
	Object.entries(params).forEach(([key, value]) => {
		if (value) url.searchParams.set(key, value)
	})
	return url.toString()
}

const fetchBusTimeResponse = async <T>(endpoint: string, params: Record<string, string>) => {
	const url = buildUrl(endpoint, params)
	const res = await fetch(url)
	if (!res.ok) {
		throw new CtaBusTrackerError({
			message: `CTA Bus Tracker HTTP error ${res.status}`,
			endpoint,
			status: res.status
		})
	}
	const payload = (await res.json()) as BusTimeResponse<BusTimeResponseBody<T>>
	if (!payload['bustime-response']) {
		throw new CtaBusTrackerError({
			message: 'CTA Bus Tracker response missing payload',
			endpoint,
			status: res.status
		})
	}
	const response = payload['bustime-response']
	const errors = Array.isArray(response.error) ? response.error : []
	return { response, errors, status: res.status }
}

export const busTimeRequest = async <T>(endpoint: string, params: Record<string, string>) => {
	const { response, errors, status } = await fetchBusTimeResponse<T>(endpoint, params)
	if (errors.length > 0) {
		const errorMessage = errors
			.map((error) => error.msg)
			.filter((msg): msg is string => typeof msg === 'string' && msg.trim().length > 0)
			.join('; ')

		throw new CtaBusTrackerError({
			message: errorMessage
				? `CTA Bus Tracker API error: ${errorMessage}`
				: 'CTA Bus Tracker API returned an error response',
			endpoint,
			details: errors,
			status
		})
	}

	return response as T
}

// Same as busTimeRequest but does NOT throw when the API returns per-row
// "no data" / "no vehicle" errors alongside valid data — the CTA API
// commonly returns both `vehicle` and `error` in a single batch when only
// a subset of the requested routes is currently in service. Returning the
// partial response lets callers consume the vehicles instead of discarding
// the whole batch.
export const busTimeRequestTolerant = async <T>(
	endpoint: string,
	params: Record<string, string>
) => {
	const { response, errors } = await fetchBusTimeResponse<T>(endpoint, params)
	return { response: response as T, errors }
}
