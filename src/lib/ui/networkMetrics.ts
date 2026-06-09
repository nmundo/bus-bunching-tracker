import type { RouteStat } from '$lib/types/frontend'

export const HIGH_RISK_THRESHOLD = 0.2
export const MEDIUM_RISK_THRESHOLD = 0.1

// Rates and waits are computed over "analyzable" headways (those with a schedule
// match, within the sanity cap). A handful of analyzable headways yields a noisy
// rate — e.g. 1 of 2 reads as 50% — so this is the bar below which a route's
// numbers are flagged low-confidence and kept out of the headline "worst route".
export const LOW_CONFIDENCE_HEADWAYS = 30

/** Headways backing a route's schedule-relative metrics (analyzable, with fallback). */
export const confidentHeadways = (route: RouteStat): number =>
	route.analyzable_headways ?? route.total_headways ?? 0

export type RiskLevel = 'high' | 'medium' | 'low' | 'unknown'

export const classifyRisk = (value: number | null | undefined): RiskLevel => {
	if (value === null || value === undefined) {
		return 'unknown'
	}
	if (value >= HIGH_RISK_THRESHOLD) {
		return 'high'
	}
	if (value >= MEDIUM_RISK_THRESHOLD) {
		return 'medium'
	}
	return 'low'
}

export const getWorstRoute = (routes: RouteStat[]): RouteStat | null => {
	const pick = (pool: RouteStat[]): RouteStat | null => {
		let worst: RouteStat | null = null
		for (const route of pool) {
			if (route.bunching_rate === null) continue
			if (!worst || (worst.bunching_rate ?? -1) < route.bunching_rate) worst = route
		}
		return worst
	}

	// Prefer routes with enough data to trust the rate; only fall back to the full
	// set if nothing clears the bar (e.g. a sparse dataset).
	const confident = routes.filter((r) => confidentHeadways(r) >= LOW_CONFIDENCE_HEADWAYS)
	return pick(confident) ?? pick(routes)
}

export const countHighRiskRoutes = (routes: RouteStat[]): number =>
	routes.filter((route) => (route.bunching_rate ?? -1) >= HIGH_RISK_THRESHOLD).length

export const countRoutesWithData = (routes: RouteStat[]): number =>
	routes.filter((route) => route.bunching_rate !== null).length

const sumField = (routes: RouteStat[], field: keyof RouteStat): number => {
	let total = 0
	for (const route of routes) {
		const value = route[field]
		if (typeof value === 'number' && Number.isFinite(value)) {
			total += value
		}
	}
	return total
}

export type NetworkMetrics = {
	bunchingRate: number | null
	superBunchingRate: number | null
	gappingRate: number | null
	excessWaitMin: number | null
	headwayCv: number | null
}

/**
 * Pool the network-wide metrics by summing each route's sufficient statistics
 * and computing the metric once. This is exact, unlike averaging the per-route
 * rates — bunching/gapping/EWT/CV are non-linear in the underlying counts, so a
 * weighted mean of per-route values would be wrong. See $server/metricSql.
 */
export const computeNetworkMetrics = (routes: RouteStat[]): NetworkMetrics => {
	const total = sumField(routes, 'total_headways')
	const analyzable = sumField(routes, 'analyzable_headways')
	const bunched = sumField(routes, 'bunched_headways')
	const superBunched = sumField(routes, 'super_bunched_headways')
	const gapped = sumField(routes, 'gapped_headways')
	const sumActual = sumField(routes, 'sum_actual_hw')
	const sumActualSq = sumField(routes, 'sum_actual_hw_sq')
	const sumSched = sumField(routes, 'sum_sched_hw')
	const sumSchedSq = sumField(routes, 'sum_sched_hw_sq')

	const observedWait = sumActual > 0 ? sumActualSq / (2 * sumActual) : null
	const scheduledWait = sumSched > 0 ? sumSchedSq / (2 * sumSched) : null

	let headwayCv: number | null = null
	if (analyzable >= 2 && sumActual > 0) {
		const mean = sumActual / analyzable
		const variance = (sumActualSq - (sumActual * sumActual) / analyzable) / (analyzable - 1)
		headwayCv = Math.sqrt(Math.max(variance, 0)) / mean
	}

	return {
		bunchingRate: analyzable > 0 ? bunched / analyzable : null,
		superBunchingRate: total > 0 ? superBunched / total : null,
		gappingRate: analyzable > 0 ? gapped / analyzable : null,
		excessWaitMin:
			observedWait !== null && scheduledWait !== null ? observedWait - scheduledWait : null,
		headwayCv
	}
}
