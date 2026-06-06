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
			const haystack = [route.route_short_name, route.route_long_name ?? '', route.route_id]
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

export type RouteSortCol = 'route' | 'bunching_rate' | 'total_headways' | 'avg_hw_ratio'
export type RouteSortDir = 'asc' | 'desc'

const VALID_SORT_COLS: RouteSortCol[] = ['route', 'bunching_rate', 'total_headways', 'avg_hw_ratio']
const VALID_SORT_DIRS: RouteSortDir[] = ['asc', 'desc']

export const parseSortParams = (
	searchParams: URLSearchParams
): { sortCol: RouteSortCol; sortDir: RouteSortDir } => {
	const col = searchParams.get('sort') as RouteSortCol | null
	const dir = searchParams.get('sort_dir') as RouteSortDir | null
	return {
		sortCol: col && VALID_SORT_COLS.includes(col) ? col : 'bunching_rate',
		sortDir: dir && VALID_SORT_DIRS.includes(dir) ? dir : 'desc'
	}
}

export const sortRoutes = (
	routes: RouteStat[],
	col: RouteSortCol,
	dir: RouteSortDir
): RouteStat[] => {
	const sign = dir === 'asc' ? 1 : -1
	return [...routes].sort((a, b) => {
		let av: string | number | null
		let bv: string | number | null
		if (col === 'route') {
			av = a.route_short_name || a.route_id
			bv = b.route_short_name || b.route_id
			return sign * av.localeCompare(bv, undefined, { numeric: true })
		}
		av = a[col]
		bv = b[col]
		if (av === null && bv === null) return 0
		if (av === null) return 1
		if (bv === null) return -1
		return sign * ((av as number) - (bv as number))
	})
}

export const withRouteTableFilterParams = (
	searchParams: URLSearchParams,
	filters: RouteTableFilters,
	sort?: { sortCol: RouteSortCol; sortDir: RouteSortDir }
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

	if (sort) {
		if (sort.sortCol !== 'bunching_rate' || sort.sortDir !== 'desc') {
			next.set('sort', sort.sortCol)
			next.set('sort_dir', sort.sortDir)
		} else {
			next.delete('sort')
			next.delete('sort_dir')
		}
	}

	return next
}
