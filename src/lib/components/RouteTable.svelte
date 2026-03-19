<script lang="ts">
	import { goto } from '$app/navigation'
	import type { RouteStat } from '$lib/types/frontend'
	import { buildRouteDetailHref } from '$lib/ui/routeDetailUrl'
	import { classifyRisk, type RiskLevel } from '$lib/ui/networkMetrics'

	let {
		routes = [],
		serviceId = '',
		bucket = ''
	}: {
		routes?: RouteStat[]
		serviceId?: string
		bucket?: string
	} = $props()

	const formatPercent = (value: number | null) =>
		value === null ? '—' : `${(value * 100).toFixed(1)}%`
	const formatNumber = (value: number | null) => (value === null ? '—' : value.toFixed(2))
	const formatHeadways = (value: number | null) => (value === null ? '—' : value.toLocaleString())
	const getRiskLevel = (value: number | null): RiskLevel => classifyRisk(value)
	const getRouteHref = (routeId: string) => buildRouteDetailHref(routeId, { serviceId, bucket })
</script>

<div class="table-wrap">
	<table class="table">
		<thead>
			<tr>
				<th>Route</th>
				<th>Name</th>
				<th>Bunching rate</th>
				<th>Total data points</th>
				<th>Avg ratio</th>
			</tr>
		</thead>
		<tbody>
			{#if routes.length === 0}
				<tr>
					<td colspan="5" class="table-empty">
						No route stats available for the selected filters.
					</td>
				</tr>
			{:else}
				{#each routes as route (route.route_id)}
					<tr
						class="route-row"
						role="link"
						tabindex="0"
						onclick={() => goto(getRouteHref(route.route_id))}
						onkeydown={(event) => {
							if (event.key === 'Enter' || event.key === ' ') {
								event.preventDefault()
								goto(getRouteHref(route.route_id))
							}
						}}
					>
						<td>
							<span class="route-link">{route.route_short_name || route.route_id}</span>
						</td>
						<td>{route.route_long_name ?? '—'}</td>
						<td>
							{#if route.bunching_rate === null}
								—
							{:else}
								<span class={`risk-pill ${getRiskLevel(route.bunching_rate)}`}>
									{formatPercent(route.bunching_rate)}
								</span>
							{/if}
						</td>
						<td>{formatHeadways(route.total_headways)}</td>
						<td>{formatNumber(route.avg_hw_ratio)}</td>
					</tr>
				{/each}
			{/if}
		</tbody>
	</table>
</div>
