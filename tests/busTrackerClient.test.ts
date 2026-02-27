import { afterEach, describe, expect, it, vi } from 'vitest'
import { CtaBusTrackerError, busTimeRequest } from '../worker/src/busTrackerClient'

const mockResponse = (payload: unknown, status = 200) =>
	({
		ok: status >= 200 && status < 300,
		status,
		json: async () => payload
	}) as Response

describe('busTimeRequest', () => {
	const originalFetch = globalThis.fetch
	const originalApiKey = process.env.CTA_BUS_TRACKER_API_KEY

	afterEach(() => {
		vi.restoreAllMocks()
		globalThis.fetch = originalFetch
		process.env.CTA_BUS_TRACKER_API_KEY = originalApiKey
	})

	it('throws a typed error when CTA returns payload errors', async () => {
		process.env.CTA_BUS_TRACKER_API_KEY = 'test-key'
		globalThis.fetch = vi.fn().mockResolvedValue(
			mockResponse({
				'bustime-response': {
					error: [{ msg: 'Maximum number of rt identifiers exceeded' }]
				}
			})
		) as typeof fetch

		await expect(busTimeRequest('getvehicles', { rt: '1,2,3' })).rejects.toMatchObject({
			name: 'CtaBusTrackerError',
			message: expect.stringContaining('Maximum number of rt identifiers exceeded'),
			endpoint: 'getvehicles'
		})
		await expect(busTimeRequest('getvehicles', { rt: '1,2,3' })).rejects.toBeInstanceOf(
			CtaBusTrackerError
		)
	})

	it('throws a typed error for non-2xx responses', async () => {
		process.env.CTA_BUS_TRACKER_API_KEY = 'test-key'
		globalThis.fetch = vi.fn().mockResolvedValue(mockResponse({}, 503)) as typeof fetch

		await expect(busTimeRequest('getvehicles', { rt: '22' })).rejects.toMatchObject({
			name: 'CtaBusTrackerError',
			message: 'CTA Bus Tracker HTTP error 503',
			status: 503
		})
	})
})
