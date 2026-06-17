<script lang="ts">
	import { goto } from '$app/navigation'
	import { fade } from 'svelte/transition'
	import { flip } from 'svelte/animate'
	import type { RouteStat } from '$lib/types/frontend'
	import { buildRouteDetailHref } from '$lib/ui/routeDetailUrl'
	import {
		classifyRisk,
		confidentHeadways,
		EWT_FREQUENT_HEADWAY_MAX,
		LOW_CONFIDENCE_HEADWAYS,
		type RiskLevel
	} from '$lib/ui/networkMetrics'
	import type { RouteSortCol, RouteSortDir } from '$lib/ui/routeTableFilters'

	let {
		routes = [],
		serviceId = '',
		bucket = '',
		sortCol = 'bunching_rate',
		sortDir = 'desc',
		onSort
	}: {
		routes?: RouteStat[]
		serviceId?: string
		bucket?: string
		sortCol?: RouteSortCol
		sortDir?: RouteSortDir
		onSort?: (col: RouteSortCol) => void
	} = $props()

	// EWT is computed only over frequent service upstream (see EWT_FREQUENT_HEADWAY_MAX);
	// we additionally suppress the value for routes whose mean scheduled headway exceeds
	// the threshold, i.e. routes that aren't predominantly turn-up-and-go.

	// Rates computed from very few observations are noisy; mark them so a 100%
	// rate from 3 headways doesn't read the same as one from 300. Confidence is
	// judged on the analyzable headways that actually back the rate, not the raw
	// total (a route can have many headways but few with a schedule match).
	const isLowConfidence = (route: RouteStat) => {
		const n = confidentHeadways(route)
		return n > 0 && n < LOW_CONFIDENCE_HEADWAYS
	}

	const formatPercent = (value: number | null) =>
		value === null ? '—' : `${(value * 100).toFixed(1)}%`
	const formatHeadways = (value: number | null) => (value === null ? '—' : value.toLocaleString())
	const formatCv = (value: number | null | undefined) =>
		value === null || value === undefined ? '—' : value.toFixed(2)
	const formatExcessWait = (route: RouteStat) => {
		const ewt = route.excess_wait_min
		const sched = route.mean_scheduled_headway
		if (ewt === null || ewt === undefined) return '—'
		if (sched === null || sched === undefined || sched > EWT_FREQUENT_HEADWAY_MAX) return '—'
		// Floored at 0: negative excess wait (better than schedule) reads oddly.
		return `+${Math.max(0, ewt).toFixed(1)}`
	}
	const getRiskLevel = (value: number | null): RiskLevel => classifyRisk(value)
	const getRouteHref = (routeId: string) => buildRouteDetailHref(routeId, { serviceId, bucket })

	const sortIcon = (col: RouteSortCol) => {
		if (col !== sortCol) return '↕'
		return sortDir === 'asc' ? '↑' : '↓'
	}

	const BUCKET_LABELS: Record<string, string> = {
		AM_peak: 'AM Peak',
		Midday: 'Midday',
		PM_peak: 'PM Peak',
		Evening: 'Evening',
		Night: 'Night'
	}
	const formatBucket = (bucket: string | null) =>
		bucket ? (BUCKET_LABELS[bucket] ?? bucket) : null
</script>

<div class="table-wrap">
	<table class="table">
		<thead>
			<tr>
				<th
					class="sortable-th"
					onclick={() => onSort?.('route')}
					aria-sort={sortCol === 'route'
						? sortDir === 'asc'
							? 'ascending'
							: 'descending'
						: 'none'}>Route<span class="sort-icon">{sortIcon('route')}</span></th
				>
				<th>Name</th>
				<th
					class="sortable-th"
					onclick={() => onSort?.('bunching_rate')}
					aria-sort={sortCol === 'bunching_rate'
						? sortDir === 'asc'
							? 'ascending'
							: 'descending'
						: 'none'}>Bunching rate<span class="sort-icon">{sortIcon('bunching_rate')}</span></th
				>
				<th class="th-badge">Peak period</th>
				<th
					class="sortable-th"
					onclick={() => onSort?.('total_headways')}
					aria-sort={sortCol === 'total_headways'
						? sortDir === 'asc'
							? 'ascending'
							: 'descending'
						: 'none'}>Headways<span class="sort-icon">{sortIcon('total_headways')}</span></th
				>
				<th
					class="sortable-th"
					onclick={() => onSort?.('excess_wait_min')}
					aria-sort={sortCol === 'excess_wait_min'
						? sortDir === 'asc'
							? 'ascending'
							: 'descending'
						: 'none'}
					title="Excess wait time (min): extra minutes the average rider waits versus the schedule, from headway irregularity. Shown only for frequent (turn-up-and-go) routes."
					>Excess wait<span class="sort-icon">{sortIcon('excess_wait_min')}</span></th
				>
				<th
					class="sortable-th"
					onclick={() => onSort?.('headway_cv')}
					aria-sort={sortCol === 'headway_cv'
						? sortDir === 'asc'
							? 'ascending'
							: 'descending'
						: 'none'}
					title="Coefficient of variation of headways (std dev ÷ mean). 0 = perfectly even spacing; higher = more irregular. The transit-industry standard regularity measure."
					>Headway CV<span class="sort-icon">{sortIcon('headway_cv')}</span></th
				>
			</tr>
		</thead>
		<tbody>
			{#if routes.length === 0}
				<tr>
					<td colspan="7" class="table-empty">
						No route stats available for the selected filters.
					</td>
				</tr>
			{:else}
				{#each routes as route (route.route_id)}
					<tr
						class="route-row"
						class:low-confidence={isLowConfidence(route)}
						title={isLowConfidence(route)
							? `Based on only ${confidentHeadways(route)} schedule-matched headways — interpret with caution.`
							: undefined}
						animate:flip={{ duration: 260 }}
						in:fade={{ duration: 180 }}
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
						<td>
							{#if formatBucket(route.worst_bucket ?? null)}
								<span class="bucket-badge">{formatBucket(route.worst_bucket ?? null)}</span>
							{:else}
								—
							{/if}
						</td>
						<td>
							{formatHeadways(route.total_headways)}
							{#if isLowConfidence(route)}<span class="low-data-flag" aria-hidden="true">⚠</span>{/if}
						</td>
						<td>{formatExcessWait(route)}</td>
						<td>{formatCv(route.headway_cv)}</td>
					</tr>
				{/each}
			{/if}
		</tbody>
	</table>
</div>

<style>
	.sortable-th {
		cursor: pointer;
		user-select: none;
		white-space: nowrap;
	}
	.sortable-th:hover {
		background: var(--surface-2);
	}
	.sort-icon {
		display: inline-block;
		margin-left: 5px;
		font-size: 9px;
		opacity: 0.35;
		vertical-align: middle;
	}
	.sortable-th[aria-sort='ascending'] .sort-icon,
	.sortable-th[aria-sort='descending'] .sort-icon {
		opacity: 1;
		color: var(--text-strong);
	}
	.th-badge {
		white-space: nowrap;
	}
	.route-row.low-confidence {
		opacity: 0.62;
	}
	.low-data-flag {
		margin-left: 4px;
		font-size: 11px;
		color: var(--risk-medium, #b54708);
		cursor: help;
	}
	.bucket-badge {
		display: inline-flex;
		align-items: center;
		padding: 2px 8px;
		border-radius: 999px;
		border: 1px solid #d6dde6;
		background: var(--surface-2);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.05em;
		color: var(--text-muted);
		white-space: nowrap;
	}
</style>
