import type { RouteStat } from '$lib/types/frontend'
import { classifyRisk } from '$lib/ui/networkMetrics'

export type RouteRiskFilter = 'all' | 'high' | 'medium' | 'low' | 'unknown'

export type RouteTableFilters = {
	q: string
	risk: RouteRiskFilter
	minData: number
}

export const DEFAULT_ROUTE_TABLE_FILTERS: RouteTableFilters = {
	q: '',
	risk: 'all',
	minData: 0
}

const VALID_RISK_FILTERS: RouteRiskFilter[] = ['all', 'high', 'medium', 'low', 'unknown']

const parseMinData = (value: string | null): number => {
	if (!value) {
		return DEFAULT_ROUTE_TABLE_FILTERS.minData
	}
	const parsed = Number.parseInt(value, 10)
	if (!Number.isFinite(parsed) || parsed < 0) {
		return DEFAULT_ROUTE_TABLE_FILTERS.minData
	}
	return parsed
}

const parseRiskFilter = (value: string | null): RouteRiskFilter => {
	if (!value) {
		return DEFAULT_ROUTE_TABLE_FILTERS.risk
	}
	return VALID_RISK_FILTERS.includes(value as RouteRiskFilter)
		? (value as RouteRiskFilter)
		: DEFAULT_ROUTE_TABLE_FILTERS.risk
}

export const parseRouteTableFilters = (searchParams: URLSearchParams): RouteTableFilters => ({
	q: (searchParams.get('q') ?? '').trim(),
	risk: parseRiskFilter(searchParams.get('risk')),
	minData: parseMinData(searchParams.get('min_data'))
})

export const applyRouteTableFilters = (
	routes: RouteStat[],
	filters: RouteTableFilters
): RouteStat[] => {
	const query = filters.q.toLowerCase()

	return routes.filter((route) => {
		if (query) {
			const haystack = [
				route.route_short_name,
				route.route_long_name ?? '',
				route.route_id
			]
				.join(' ')
				.toLowerCase()

			if (!haystack.includes(query)) {
				return false
			}
		}

		if (filters.risk !== 'all' && classifyRisk(route.bunching_rate) !== filters.risk) {
			return false
		}

		if (filters.minData > 0 && (route.total_headways ?? 0) < filters.minData) {
			return false
		}

		return true
	})
}

export const withRouteTableFilterParams = (
	searchParams: URLSearchParams,
	filters: RouteTableFilters
): URLSearchParams => {
	const next = new URLSearchParams(searchParams)

	if (filters.q) {
		next.set('q', filters.q)
	} else {
		next.delete('q')
	}

	if (filters.risk !== DEFAULT_ROUTE_TABLE_FILTERS.risk) {
		next.set('risk', filters.risk)
	} else {
		next.delete('risk')
	}

	if (filters.minData > DEFAULT_ROUTE_TABLE_FILTERS.minData) {
		next.set('min_data', String(filters.minData))
	} else {
		next.delete('min_data')
	}

	return next
}
