<script lang="ts">
	import { browser } from '$app/environment'
	import RouteTable from '$components/RouteTable.svelte'
	import type { RouteStat } from '$lib/types/frontend'
	import {
		computeWeightedNetworkAverage,
		countHighRiskRoutes,
		countRoutesWithData,
		getWorstRoute
	} from '$lib/ui/networkMetrics'
	import {
		applyRouteTableFilters,
		type RouteRiskFilter,
		withRouteTableFilterParams
	} from '$lib/ui/routeTableFilters'
	import type { PageData } from './$types'

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

	const timeBuckets = ['AM_peak', 'Midday', 'PM_peak', 'Evening', 'Night']

	const formatPercent = (value: number | null) =>
		value === null ? '—' : `${(value * 100).toFixed(1)}%`
	const formatBucketLabel = (value: string) => value.replace('_', ' ')

	$effect(() => {
		serviceId = data.serviceId
		bucket = data.bucket
		routes = data.routes
		q = data.q
		risk = data.risk
		minData = data.minData
	})

	const tableFilters = $derived.by(() => ({
		q: q.trim(),
		risk,
		minData: Number.isFinite(minData) && minData > 0 ? Math.floor(minData) : 0
	}))

	const visibleRoutes = $derived.by(() => applyRouteTableFilters(routes, tableFilters))

	$effect(() => {
		if (!browser) {
			return
		}

		const next = withRouteTableFilterParams(
			new URLSearchParams(window.location.search),
			tableFilters
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

		return {
			worstRouteLabel: worstRoute?.route_short_name || worstRoute?.route_id || '—',
			worstRouteName: worstRoute?.route_long_name ?? 'No route data',
			worstRate: formatPercent(worstRoute?.bunching_rate ?? null),
			networkAverage: formatPercent(networkAverage),
			highRiskCount,
			withDataCount,
			totalRoutes: routes.length
		}
	})

	const refresh = async () => {
		loading = true
		const params = new URLSearchParams()
		if (serviceId) params.set('service_id', serviceId)
		if (bucket) params.set('time_of_day_bucket', bucket)
		const res = await fetch(`/api/routes?${params.toString()}`)
		routes = res.ok ? await res.json() : []
		loading = false
	}
</script>

<section class="grid">
	<div class="kpi-grid">
		<article class="stat-card">
			<div>
				<p class="meta-line">Network overview</p>
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
				<button onclick={refresh} disabled={loading} aria-label="Submit filters">&rarr;</button>
				<!-- {#if loading}
					<small class="mono loading-indicator">Loading…</small>
				{/if} -->
			</div>
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
	</div>

	<div class="panel">
		<div class="section-head">
			<div>
				<p class="meta-line">Performance ranking</p>
				<h3>Route bunching table</h3>
				<small class="mono">Ordered by bunching rate · {visibleRoutes.length} shown</small>
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
		<RouteTable routes={visibleRoutes} />
	</div>
</section>
