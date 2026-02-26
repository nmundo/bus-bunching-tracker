import type { RouteStat } from '$lib/types/frontend'

export const HIGH_RISK_THRESHOLD = 0.2
export const MEDIUM_RISK_THRESHOLD = 0.1

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

export const computeWeightedNetworkAverage = (routes: RouteStat[]): number | null => {
	let weightedTotal = 0
	let totalHeadways = 0

	for (const route of routes) {
		if (
			route.bunching_rate === null ||
			route.total_headways === null ||
			route.total_headways <= 0
		) {
			continue
		}
		weightedTotal += route.bunching_rate * route.total_headways
		totalHeadways += route.total_headways
	}

	if (totalHeadways > 0) {
		return weightedTotal / totalHeadways
	}

	let unweightedTotal = 0
	let unweightedCount = 0

	for (const route of routes) {
		if (route.bunching_rate === null) {
			continue
		}
		unweightedTotal += route.bunching_rate
		unweightedCount += 1
	}

	return unweightedCount > 0 ? unweightedTotal / unweightedCount : null
}

export const getWorstRoute = (routes: RouteStat[]): RouteStat | null => {
	let worst: RouteStat | null = null

	for (const route of routes) {
		if (route.bunching_rate === null) {
			continue
		}
		if (!worst || (worst.bunching_rate ?? -1) < route.bunching_rate) {
			worst = route
		}
	}

	return worst
}

export const countHighRiskRoutes = (routes: RouteStat[]): number =>
	routes.filter((route) => (route.bunching_rate ?? -1) >= HIGH_RISK_THRESHOLD).length

export const countRoutesWithData = (routes: RouteStat[]): number =>
	routes.filter((route) => route.bunching_rate !== null).length
