import { describe, expect, it } from 'vitest'
import {
	MAX_ROUTES_PER_REQUEST,
	buildPollPlan,
	clampBatchSize,
	getLowTierCycleMultiplier,
	stableRouteBucket,
	type RouteActivityState
} from '../worker/src/busTrackerPoller'

const makeRouteActivity = (pairs: Array<[string, number]>) => {
	const activity = new Map<string, RouteActivityState>()
	for (const [routeId, lastSeenEpochSec] of pairs) {
		activity.set(routeId, { lastSeenEpochSec })
	}
	return activity
}

describe('busTrackerPoller helpers', () => {
	it('hard caps batch size to CTA max routes per request', () => {
		expect(clampBatchSize(30)).toBe(MAX_ROUTES_PER_REQUEST)
		expect(clampBatchSize(10)).toBe(10)
		expect(clampBatchSize(0)).toBe(1)
	})

	it('computes slow-tier cycle multiplier from interval and staleness', () => {
		expect(getLowTierCycleMultiplier(360, 120)).toBe(3)
		expect(getLowTierCycleMultiplier(360, 361)).toBe(1)
	})

	it('assigns routes to deterministic slow buckets across cycles', () => {
		const routes = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
		const routeActivity = makeRouteActivity([])
		const seen = new Set<string>()

		for (let cycleIndex = 0; cycleIndex < 3; cycleIndex += 1) {
			const plan = buildPollPlan({
				routes,
				routeActivity,
				nowEpochSec: 1000,
				lowTierMaxStalenessSec: 360,
				lowTierCycleMultiplier: 3,
				cycleIndex
			})
			expect(plan.fastRoutes).toHaveLength(0)
			for (const routeId of plan.allRoutesThisCycle) {
				seen.add(routeId)
			}
		}

		expect(seen).toEqual(new Set(routes))
	})

	it('promotes active routes to fast tier and demotes stale routes', () => {
		const routes = ['22', '49']
		const initialNow = 1000
		const routeActivity = makeRouteActivity([
			['22', initialNow - 60],
			['49', initialNow - 500]
		])

		const activePlan = buildPollPlan({
			routes,
			routeActivity,
			nowEpochSec: initialNow,
			lowTierMaxStalenessSec: 360,
			lowTierCycleMultiplier: 3,
			cycleIndex: 0
		})
		expect(activePlan.fastRoutes).toContain('22')
		expect(activePlan.fastRoutes).not.toContain('49')

		const stalePlan = buildPollPlan({
			routes,
			routeActivity,
			nowEpochSec: initialNow + 400,
			lowTierMaxStalenessSec: 360,
			lowTierCycleMultiplier: 3,
			cycleIndex: 1
		})
		expect(stalePlan.fastRoutes).not.toContain('22')
	})

	it('keeps bucket hashing stable for the same route id', () => {
		expect(stableRouteBucket('22', 3)).toBe(stableRouteBucket('22', 3))
		expect(stableRouteBucket('49', 3)).toBe(stableRouteBucket('49', 3))
	})
})
