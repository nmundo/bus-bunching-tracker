import { describe, expect, it } from 'vitest'
import {
	classifyRisk,
	computeNetworkMetrics,
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

describe('computeNetworkMetrics', () => {
	it('pools rates from summed counts rather than averaging per-route rates', () => {
		const routes = [
			makeRoute({
				route_id: '8',
				total_headways: 300,
				analyzable_headways: 300,
				bunched_headways: 90, // 30%
				super_bunched_headways: 30,
				gapped_headways: 15
			}),
			makeRoute({
				route_id: '3',
				total_headways: 100,
				analyzable_headways: 100,
				bunched_headways: 10, // 10%
				super_bunched_headways: 5,
				gapped_headways: 5
			})
		]

		const network = computeNetworkMetrics(routes)
		// (90 + 10) / (300 + 100) = 0.25
		expect(network.bunchingRate).toBeCloseTo(0.25, 6)
		// (30 + 5) / (300 + 100) = 0.0875
		expect(network.superBunchingRate).toBeCloseTo(0.0875, 6)
		// (15 + 5) / 400 = 0.05
		expect(network.gappingRate).toBeCloseTo(0.05, 6)
	})

	it('pools excess wait from Σh / Σh² (E[H²]/2E[H])', () => {
		// Two analyzable headways of 10 min each, scheduled 10 min each.
		// observed wait = (100 + 100) / (2 * (10 + 10)) = 5; scheduled wait = 5; excess = 0.
		const routes = [
			makeRoute({
				route_id: '1',
				total_headways: 2,
				analyzable_headways: 2,
				sum_actual_hw: 20,
				sum_actual_hw_sq: 200,
				sum_sched_hw: 20,
				sum_sched_hw_sq: 200
			})
		]
		const network = computeNetworkMetrics(routes)
		expect(network.excessWaitMin).toBeCloseTo(0, 6)
	})

	it('returns nulls when there are no analyzable headways', () => {
		const routes = [makeRoute({ total_headways: 0, analyzable_headways: 0 })]
		const network = computeNetworkMetrics(routes)
		expect(network.bunchingRate).toBeNull()
		expect(network.gappingRate).toBeNull()
		expect(network.excessWaitMin).toBeNull()
		expect(network.headwayCv).toBeNull()
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
