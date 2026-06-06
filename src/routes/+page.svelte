<script lang="ts">
	import { browser } from '$app/environment'
	import { fly } from 'svelte/transition'
	import RouteTable from '$components/RouteTable.svelte'
	import BunchingChart from '$components/BunchingChart.svelte'
	import type { RouteStat } from '$lib/types/frontend'
	import {
		computeWeightedNetworkAverage,
		computeWeightedRate,
		countHighRiskRoutes,
		countRoutesWithData,
		getWorstRoute
	} from '$lib/ui/networkMetrics'
	import {
		applyRouteTableFilters,
		sortRoutes,
		type RouteRiskFilter,
		type RouteSortCol,
		type RouteSortDir,
		withRouteTableFilterParams
	} from '$lib/ui/routeTableFilters'
	import { withRouteDetailFilterParams } from '$lib/ui/routeDetailUrl'
	import type { PageData } from './$types'
	import { getRoutes, getNetworkHourly } from './data.remote'

	type Props = {
		data: PageData
	}

	let { data }: Props = $props()

	let serviceId = $state('')
	let bucket = $state('')
	let routes = $state<RouteStat[]>([])
	let q = $state('')
	let risk = $state<RouteRiskFilter>('all')
	let minData = $state(0)
	let loading = $state(false)
	let sortCol = $state<RouteSortCol>(data.sortCol)
	let sortDir = $state<RouteSortDir>(data.sortDir)
	let dataWatermark = $state<string | null>(null)
	let networkHourly = $state<import('$lib/types/frontend').BucketStat[]>([])

	const timeBuckets = ['AM_peak', 'Midday', 'PM_peak', 'Evening', 'Night']

	const formatPercent = (value: number | null) =>
		value === null ? '—' : `${(value * 100).toFixed(1)}%`
	const formatMinutes = (value: number | null) =>
		value === null ? '—' : `${value >= 0 ? '+' : ''}${value.toFixed(1)} min`
	const formatBucketLabel = (value: string) => value.replace('_', ' ')

	$effect(() => {
		serviceId = data.serviceId
		bucket = data.bucket
		routes = data.routes
		q = data.q
		risk = data.risk
		minData = data.minData
		sortCol = data.sortCol
		sortDir = data.sortDir
		dataWatermark = data.watermark ?? null
		networkHourly = data.networkHourly ?? []
	})

	const tableFilters = $derived.by(() => ({
		q: q.trim(),
		risk,
		minData: Number.isFinite(minData) && minData > 0 ? Math.floor(minData) : 0
	}))

	const filteredRoutes = $derived.by(() => applyRouteTableFilters(routes, tableFilters))
	const visibleRoutes = $derived.by(() => sortRoutes(filteredRoutes, sortCol, sortDir))

	$effect(() => {
		if (!browser) {
			return
		}

		const next = withRouteTableFilterParams(
			new URLSearchParams(window.location.search),
			tableFilters,
			{ sortCol, sortDir }
		)
		const nextQuery = next.toString()
		const currentQuery = window.location.search.replace(/^\?/, '')

		if (nextQuery === currentQuery) {
			return
		}

		const nextUrl = nextQuery
			? `${window.location.pathname}?${nextQuery}`
			: window.location.pathname
		window.history.replaceState(window.history.state, '', nextUrl)
	})

	const dashboardMetrics = $derived.by(() => {
		const worstRoute = getWorstRoute(routes)
		const networkAverage = computeWeightedNetworkAverage(routes)
		const highRiskCount = countHighRiskRoutes(routes)
		const withDataCount = countRoutesWithData(routes)
		const networkSuperBunched = computeWeightedRate(routes, 'super_bunching_rate')
		const networkGapping = computeWeightedRate(routes, 'gapping_rate')
		const networkExcessWait = computeWeightedRate(routes, 'excess_wait_min')

		return {
			worstRouteLabel: worstRoute?.route_short_name || worstRoute?.route_id || '—',
			worstRouteName: worstRoute?.route_long_name ?? 'No route data',
			worstRate: formatPercent(worstRoute?.bunching_rate ?? null),
			networkAverage: formatPercent(networkAverage),
			networkSuperBunched: formatPercent(networkSuperBunched),
			networkGapping: formatPercent(networkGapping),
			networkExcessWait: formatMinutes(networkExcessWait),
			highRiskCount,
			withDataCount,
			totalRoutes: routes.length
		}
	})

	$effect(() => {
		if (!browser) {
			return
		}

		const next = withRouteDetailFilterParams(new URLSearchParams(window.location.search), {
			serviceId,
			bucket
		})
		const nextQuery = next.toString()
		const currentQuery = window.location.search.replace(/^\?/, '')

		if (nextQuery === currentQuery) {
			return
		}

		const nextUrl = nextQuery
			? `${window.location.pathname}?${nextQuery}`
			: window.location.pathname
		window.history.replaceState(window.history.state, '', nextUrl)
	})

	const handleSort = (col: RouteSortCol) => {
		if (sortCol === col) {
			sortDir = sortDir === 'asc' ? 'desc' : 'asc'
		} else {
			sortCol = col
			sortDir = col === 'route' ? 'asc' : 'desc'
		}
	}

	const relativeTime = (isoString: string | null): string => {
		if (!isoString) return ''
		const diff = Math.floor((Date.now() - new Date(isoString).getTime()) / 60_000)
		if (diff < 1) return 'just now'
		if (diff === 1) return '1 min ago'
		if (diff < 60) return `${diff} min ago`
		const hours = Math.floor(diff / 60)
		return `${hours}h ${diff % 60}m ago`
	}

	let freshnessLabel = $state('')
	$effect(() => {
		if (!browser) return
		freshnessLabel = relativeTime(dataWatermark)
		const interval = setInterval(() => {
			freshnessLabel = relativeTime(dataWatermark)
		}, 60_000)
		return () => clearInterval(interval)
	})


	const refresh = async () => {
		loading = true
		const rq = getRoutes({ serviceId, bucket })
		const hq = getNetworkHourly({ serviceId })
		await Promise.all([rq.refresh(), hq.refresh()])
		routes = rq.current ?? []
		networkHourly = hq.current ?? []
		loading = false
	}
</script>

<section class="grid">
	<div class="page-toolbar">
		<div class="toolbar-identity">
			<p class="meta-line">Network overview</p>
			{#if freshnessLabel}
				<small class="mono freshness-label">Data as of {freshnessLabel}</small>
			{/if}
		</div>
		<div class="controls-row">
			<label class="control-field">
				<span>Service type</span>
				<select bind:value={serviceId}>
					<option value="">all services</option>
					<option value="weekday">weekday</option>
					<option value="saturday">saturday</option>
					<option value="sunday">sunday</option>
				</select>
			</label>
			<label class="control-field">
				<span>Time bucket</span>
				<select bind:value={bucket}>
					{#each timeBuckets as option (option)}
						<option value={option}>{formatBucketLabel(option)}</option>
					{/each}
				</select>
			</label>
			<button onclick={refresh} disabled={loading} aria-label="Refresh data">&rarr;</button>
		</div>
	</div>

	<div class:stale={loading}>
		<div class="kpi-grid">
			<article class="stat-card">
				<p class="meta-line">Network excess wait</p>
				<h3>{dashboardMetrics.networkExcessWait}</h3>
				<p>Extra minutes riders wait vs. schedule</p>
			</article>
			<article class="stat-card">
				<p class="meta-line">Network avg bunching</p>
				<h3>{dashboardMetrics.networkAverage}</h3>
				<p>Weighted by observed headways</p>
			</article>
			<article class="stat-card">
				<p class="meta-line">High-risk routes</p>
				<h3>{dashboardMetrics.highRiskCount}</h3>
				<p>Bunching rate at or above 20%</p>
			</article>
			<article class="stat-card">
				<p class="meta-line">Network super-bunched</p>
				<h3>{dashboardMetrics.networkSuperBunched}</h3>
				<p>Buses within 1 min of each other</p>
			</article>
			<article class="stat-card">
				<p class="meta-line">Network gapping</p>
				<h3>{dashboardMetrics.networkGapping}</h3>
				<p>Headways &gt;175% of scheduled</p>
			</article>
		</div>

		{#if networkHourly.length > 0}
			<div class="section-gap" transition:fly={{ y: 14, duration: 300, opacity: 0 }}>
				<BunchingChart data={networkHourly} title="Network bunching by hour" />
			</div>
		{/if}

		<div class="panel section-gap">
			<div class="section-head">
				<div>
					<p class="meta-line">Performance ranking</p>
					<h3>Route bunching table</h3>
					<small class="mono">{visibleRoutes.length} routes shown</small>
				</div>
				<div class="controls-row">
					<label class="control-field">
						<span>Search route</span>
						<input
							type="search"
							placeholder="Route number or name"
							bind:value={q}
							aria-label="Search routes"
						/>
					</label>
					<label class="control-field">
						<span>Risk level</span>
						<select bind:value={risk} aria-label="Filter by risk level">
							<option value="all">all risk levels</option>
							<option value="high">high</option>
							<option value="medium">medium</option>
							<option value="low">low</option>
							<option value="unknown">unknown</option>
						</select>
					</label>
				</div>
			</div>
			<RouteTable routes={visibleRoutes} {serviceId} {bucket} {sortCol} {sortDir} onSort={handleSort} />
		</div>
	</div>
</section>

<style>
	.page-toolbar {
		display: flex;
		flex-wrap: wrap;
		align-items: flex-end;
		justify-content: space-between;
		gap: 12px 16px;
	}
	.toolbar-identity {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}
	.freshness-label {
		color: var(--text-muted);
		font-size: 11px;
	}
</style>
