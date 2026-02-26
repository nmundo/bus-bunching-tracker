<script lang="ts">
	import type { RouteStat } from '$lib/types/frontend'

	let { routes = [] }: { routes?: RouteStat[] } = $props()

	const formatPercent = (value: number | null) =>
		value === null ? '—' : `${(value * 100).toFixed(1)}%`
	const formatNumber = (value: number | null) => (value === null ? '—' : value.toFixed(2))
</script>

<table class="table">
	<thead>
		<tr>
			<th>Route</th>
			<th>Name</th>
			<th>Bunching rate</th>
			<th>Total headways</th>
			<th>Avg ratio</th>
		</tr>
	</thead>
	<tbody>
		{#if routes.length === 0}
			<tr>
				<td colspan="5">No data yet.</td>
			</tr>
		{:else}
			{#each routes as route (route.route_id)}
				<tr>
					<td>
						<a href={`/route/${route.route_id}`}>{route.route_short_name || route.route_id}</a>
					</td>
					<td>{route.route_long_name ?? '—'}</td>
					<td>{formatPercent(route.bunching_rate)}</td>
					<td>{route.total_headways ?? '—'}</td>
					<td>{formatNumber(route.avg_hw_ratio)}</td>
				</tr>
			{/each}
		{/if}
	</tbody>
</table>
