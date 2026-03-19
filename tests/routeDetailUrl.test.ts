import { describe, expect, it } from 'vitest'
import { buildRouteDetailHref, withRouteDetailFilterParams } from '../src/lib/ui/routeDetailUrl'

describe('route detail URL filters', () => {
	it('preserves unrelated params while updating service and bucket filters', () => {
		const base = new URLSearchParams({
			q: 'halsted',
			risk: 'medium'
		})

		const serialized = withRouteDetailFilterParams(base, {
			serviceId: 'weekday',
			bucket: 'AM_peak'
		})

		expect(serialized.get('q')).toBe('halsted')
		expect(serialized.get('risk')).toBe('medium')
		expect(serialized.get('service_id')).toBe('weekday')
		expect(serialized.get('time_of_day_bucket')).toBe('AM_peak')
	})

	it('omits route-detail params when both filters are empty', () => {
		expect(
			buildRouteDetailHref('22', {
				serviceId: '',
				bucket: ''
			})
		).toBe('/route/22')
	})

	it('includes only service_id when bucket is empty', () => {
		expect(
			buildRouteDetailHref('22', {
				serviceId: 'weekday',
				bucket: ''
			})
		).toBe('/route/22?service_id=weekday')
	})

	it('includes both service_id and time_of_day_bucket when both are set', () => {
		expect(
			buildRouteDetailHref('22', {
				serviceId: 'weekday',
				bucket: 'AM_peak'
			})
		).toBe('/route/22?service_id=weekday&time_of_day_bucket=AM_peak')
	})
})
