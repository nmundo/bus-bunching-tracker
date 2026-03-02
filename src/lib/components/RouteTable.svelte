<script lang="ts">
	import type { RouteStat } from '$lib/types/frontend'
	import { classifyRisk, type RiskLevel } from '$lib/ui/networkMetrics'

	let { routes = [] }: { routes?: RouteStat[] } = $props()

	const formatPercent = (value: number | null) =>
		value === null ? '—' : `${(value * 100).toFixed(1)}%`
	const formatNumber = (value: number | null) => (value === null ? '—' : value.toFixed(2))
	const formatHeadways = (value: number | null) => (value === null ? '—' : value.toLocaleString())
	const getRiskLevel = (value: number | null): RiskLevel => classifyRisk(value)
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
					<tr>
						<td>
							<a class="route-link" href={`/route/${route.route_id}`}>
								{route.route_short_name || route.route_id}
							</a>
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
