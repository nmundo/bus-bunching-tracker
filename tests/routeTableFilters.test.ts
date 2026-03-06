import { describe, expect, it } from 'vitest'
import type { RouteStat } from '../src/lib/types/frontend'
import {
	applyRouteTableFilters,
	DEFAULT_ROUTE_TABLE_FILTERS,
	parseRouteTableFilters,
	withRouteTableFilterParams
} from '../src/lib/ui/routeTableFilters'

const makeRoute = (overrides: Partial<RouteStat>): RouteStat => ({
	route_id: '22',
	route_short_name: '22',
	route_long_name: 'Clark',
	bunching_rate: null,
	total_headways: null,
	avg_hw_ratio: null,
	...overrides
})

describe('applyRouteTableFilters', () => {
	const routes: RouteStat[] = [
		makeRoute({
			route_id: '22',
			route_short_name: '22',
			route_long_name: 'Clark',
			bunching_rate: 0.24,
			total_headways: 120
		}),
		makeRoute({
			route_id: '8',
			route_short_name: '8',
			route_long_name: 'Halsted',
			bunching_rate: 0.14,
			total_headways: 42
		}),
		makeRoute({
			route_id: '55',
			route_short_name: '55',
			route_long_name: 'Garfield',
			bunching_rate: 0.04,
			total_headways: null
		}),
		makeRoute({
			route_id: '49B',
			route_short_name: '49B',
			route_long_name: null,
			bunching_rate: null,
			total_headways: 70
		})
	]

	it('matches text across route short name, long name, and route id case-insensitively', () => {
		expect(
			applyRouteTableFilters(routes, { ...DEFAULT_ROUTE_TABLE_FILTERS, q: 'halsted' }).map(
				(route) => route.route_id
			)
		).toEqual(['8'])
		expect(
			applyRouteTableFilters(routes, { ...DEFAULT_ROUTE_TABLE_FILTERS, q: '49b' }).map(
				(route) => route.route_id
			)
		).toEqual(['49B'])
		expect(
			applyRouteTableFilters(routes, { ...DEFAULT_ROUTE_TABLE_FILTERS, q: '22' }).map(
				(route) => route.route_id
			)
		).toEqual(['22'])
	})

	it('filters by risk level using shared thresholds', () => {
		expect(
			applyRouteTableFilters(routes, { ...DEFAULT_ROUTE_TABLE_FILTERS, risk: 'high' }).map(
				(route) => route.route_id
			)
		).toEqual(['22'])
		expect(
			applyRouteTableFilters(routes, { ...DEFAULT_ROUTE_TABLE_FILTERS, risk: 'medium' }).map(
				(route) => route.route_id
			)
		).toEqual(['8'])
		expect(
			applyRouteTableFilters(routes, { ...DEFAULT_ROUTE_TABLE_FILTERS, risk: 'low' }).map(
				(route) => route.route_id
			)
		).toEqual(['55'])
		expect(
			applyRouteTableFilters(routes, { ...DEFAULT_ROUTE_TABLE_FILTERS, risk: 'unknown' }).map(
				(route) => route.route_id
			)
		).toEqual(['49B'])
	})

	it('applies minimum data-point filtering and excludes null totals when threshold is positive', () => {
		expect(
			applyRouteTableFilters(routes, { ...DEFAULT_ROUTE_TABLE_FILTERS, minData: 50 }).map(
				(route) => route.route_id
			)
		).toEqual(['22', '49B'])
	})

	it('combines filters with AND semantics', () => {
		expect(
			applyRouteTableFilters(routes, {
				q: '8',
				risk: 'medium',
				minData: 30
			}).map((route) => route.route_id)
		).toEqual(['8'])

		expect(
			applyRouteTableFilters(routes, {
				q: '8',
				risk: 'high',
				minData: 30
			})
		).toEqual([])
	})
})

describe('route table filter URL parsing/serialization', () => {
	it('parses valid values from URL params and trims query text', () => {
		const params = new URLSearchParams({
			q: '  clark  ',
			risk: 'high',
			min_data: '25'
		})
		expect(parseRouteTableFilters(params)).toEqual({
			q: 'clark',
			risk: 'high',
			minData: 25
		})
	})

	it('falls back to defaults for invalid or empty URL params', () => {
		const params = new URLSearchParams({
			q: '   ',
			risk: 'severe',
			min_data: '-2'
		})
		expect(parseRouteTableFilters(params)).toEqual(DEFAULT_ROUTE_TABLE_FILTERS)
	})

	it('round-trips to and from URL params while preserving unrelated params', () => {
		const base = new URLSearchParams({
			service_id: 'weekday',
			time_of_day_bucket: 'AM_peak'
		})

		const serialized = withRouteTableFilterParams(base, {
			q: 'halsted',
			risk: 'medium',
			minData: 40
		})

		expect(serialized.get('service_id')).toBe('weekday')
		expect(serialized.get('time_of_day_bucket')).toBe('AM_peak')
		expect(serialized.get('q')).toBe('halsted')
		expect(serialized.get('risk')).toBe('medium')
		expect(serialized.get('min_data')).toBe('40')

		expect(parseRouteTableFilters(serialized)).toEqual({
			q: 'halsted',
			risk: 'medium',
			minData: 40
		})
	})
})
