import { describe, expect, it } from 'vitest'
import {
	classifyRisk,
	computeWeightedNetworkAverage,
	countHighRiskRoutes,
	countRoutesWithData,
	getWorstRoute
} from '../src/lib/ui/networkMetrics'
import type { RouteStat } from '../src/lib/types/frontend'

const makeRoute = (overrides: Partial<RouteStat>): RouteStat => ({
	route_id: '1',
	route_short_name: '1',
	route_long_name: 'Example',
	bunching_rate: null,
	total_headways: null,
	avg_hw_ratio: null,
	...overrides
})

describe('classifyRisk', () => {
	it('classifies risk thresholds including boundaries', () => {
		expect(classifyRisk(null)).toBe('unknown')
		expect(classifyRisk(0.09)).toBe('low')
		expect(classifyRisk(0.1)).toBe('medium')
		expect(classifyRisk(0.199)).toBe('medium')
		expect(classifyRisk(0.2)).toBe('high')
	})
})

describe('computeWeightedNetworkAverage', () => {
	it('weights by total headways when available', () => {
		const routes = [
			makeRoute({ route_id: '8', bunching_rate: 0.3, total_headways: 300 }),
			makeRoute({ route_id: '3', bunching_rate: 0.1, total_headways: 100 }),
			makeRoute({ route_id: '2', bunching_rate: null, total_headways: 200 })
		]

		expect(computeWeightedNetworkAverage(routes)).toBeCloseTo(0.25, 6)
	})

	it('falls back to unweighted average when no valid weights exist', () => {
		const routes = [
			makeRoute({ route_id: '8', bunching_rate: 0.3, total_headways: null }),
			makeRoute({ route_id: '3', bunching_rate: 0.1, total_headways: 0 }),
			makeRoute({ route_id: '2', bunching_rate: null, total_headways: 20 })
		]

		expect(computeWeightedNetworkAverage(routes)).toBeCloseTo(0.2, 6)
	})
})

describe('homepage metrics', () => {
	it('finds worst route and counts risk/data', () => {
		const routes = [
			makeRoute({ route_id: '8', route_short_name: '8', bunching_rate: 0.38 }),
			makeRoute({ route_id: '3', route_short_name: '3', bunching_rate: 0.18 }),
			makeRoute({ route_id: '2', route_short_name: '2', bunching_rate: null })
		]

		expect(getWorstRoute(routes)?.route_id).toBe('8')
		expect(countHighRiskRoutes(routes)).toBe(1)
		expect(countRoutesWithData(routes)).toBe(2)
	})
})
