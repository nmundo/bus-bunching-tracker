<script lang="ts">
	import { browser, dev } from '$app/environment'
	import { fly } from 'svelte/transition'
	import BunchingChart from '$components/BunchingChart.svelte'
	import RouteMap from '$components/RouteMap.svelte'
	import RouteStatsSummary from '$components/RouteStatsSummary.svelte'
	import TrendSparkline from '$components/TrendSparkline.svelte'
	import type { TrendPoint } from '$lib/ui/trend'
	import { withRouteDetailFilterParams } from '$lib/ui/routeDetailUrl'
	import { classifyRisk, type RiskLevel } from '$lib/ui/networkMetrics'
	import { getRouteStats, getRouteSegments } from './data.remote'
	import type { PageData } from './$types'

	type Props = {
		data: PageData
	}

	let { data }: Props = $props()

	let serviceId = $state(data.serviceId)
	let bucket = $state(data.bucket)
	let stats = $state<PageData['stats']>(data.stats)
	let segments = $state<PageData['segments']>(data.segments)

	let directionId = $state(data.directionId ?? '')
	let loading = $state(false)

	const directions = $derived<Record<string, string>>(data.directions ?? {})
	const dirLabel = (id: string) => directions[id] ?? `Direction ${id}`

	const chartTitle = $derived(`Route ${stats?.route?.route_short_name ?? ''} · bunching by hour`)

	const bunchingTrend = $derived<TrendPoint[]>(
		(stats?.dailyTrend ?? []).map((d) => ({ date: d.stat_date, value: d.bunching_rate }))
	)

	const timeBuckets = ['AM_peak', 'Midday', 'PM_peak', 'Evening', 'Night']
	const formatPercent = (value: number | null | undefined) =>
		value === null || value === undefined ? '—' : `${(value * 100).toFixed(1)}%`
	const formatHeadways = (value: number | null | undefined) =>
		value === null || value === undefined ? '—' : value.toLocaleString()
	const formatBucketLabel = (value: string) => value.replace('_', ' ')
	const getRiskLevel = (value: number | null | undefined): RiskLevel => classifyRisk(value)

	$effect(() => {
		if (!browser) {
			return
		}

		const next = withRouteDetailFilterParams(new URLSearchParams(window.location.search), {
			serviceId,
			bucket
		})
		if (directionId !== '') {
			next.set('direction_id', directionId)
		} else {
			next.delete('direction_id')
		}
		const nextQuery = next.toString()
		const currentQuery = window.location.search.replace(/^\?/, '')

		if (nextQuery === currentQuery) {
			return
		}

		const nextUrl = nextQuery
			? `${window.location.pathname}?${nextQuery}`
			: window.location.pathname
		window.history.replaceState(window.history.state, '', nextUrl)
		refresh()
	})

	const refresh = async () => {
		loading = true
		const sq = getRouteStats({ routeId: data.routeId, serviceId, bucket, directionId })
		const sgq = getRouteSegments({ routeId: data.routeId, serviceId, bucket, directionId })
		await Promise.all([sq.refresh(), sgq.refresh()])
		stats = sq.current ?? null
		segments = sgq.current ?? null
		loading = false
	}

	const worstSegments = $derived.by(() => {
		if (!segments) return []
		return [...segments.features]
			.sort((a, b) => (b.properties?.bunching_rate ?? 0) - (a.properties?.bunching_rate ?? 0))
			.slice(0, 8)
	})
</script>

<section class="grid">
	<div class="detail-toolbar">
		<div class="detail-toolbar-lead">
			<a class="back-link" href="/">← Back</a>
			{#if stats?.route}
				<div class="toolbar-identity">
					<h3>
						{stats.route.route_short_name ?? 'Route'} · {stats.route.route_long_name ??
							'Route detail'}
					</h3>
					<small class="mono"
						>Total headways: {formatHeadways(stats?.summary?.total_headways)}</small
					>
				</div>
			{/if}
		</div>

		<div class="controls-row detail-controls">
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
			<label class="control-field">
				<span>Direction</span>
				<select bind:value={directionId}>
					<option value="">both directions</option>
					{#each Object.entries(directions) as [id, label] (id)}
						<option value={id}>{label}</option>
					{/each}
				</select>
			</label>
		</div>
	</div>

	<div class:stale={loading}>
		{#if stats?.summary}
			<div in:fly={{ y: 12, duration: 280, opacity: 0 }}>
				<RouteStatsSummary summary={stats.summary} />
			</div>
		{/if}

		{#if dev}
			<div class="panel section-gap trend-panel">
				<div class="section-head">
					<div>
						<p class="meta-line">Daily trend</p>
						<h3>Bunching rate over time</h3>
					</div>
				</div>
				<TrendSparkline
					points={bunchingTrend}
					label="Bunching rate"
					lowerIsBetter={true}
					width={220}
					height={44}
					format={(v) => `${(v * 100).toFixed(1)}%`}
				/>
			</div>
		{/if}

		<div class="grid two section-gap">
			<div class="panel">
				<div class="section-head">
					<div>
						<p class="meta-line">Segment ranking</p>
						<h3>Worst segments</h3>
					</div>
				</div>
				<div class="table-wrap segments-table-wrap">
					<table class="table">
						<thead>
							<tr>
								<th>Segment</th>
								<th>Bunching rate</th>
								<th>Headways</th>
							</tr>
						</thead>
						<tbody>
							{#if !segments}
								<tr>
									<td colspan="3" class="table-empty">No segments loaded for these filters.</td>
								</tr>
							{:else}
								{#each worstSegments as feature, index (feature.properties?.segment_id ?? index)}
									<tr>
										<td class="segment-cell">
											<div class="segment-stop-row">
												<span class="segment-stop-label">From</span>
												<span class="segment-stop-name"
													>{feature.properties?.from_stop_name ?? '—'}</span
												>
											</div>
											<div class="segment-stop-row">
												<span class="segment-stop-label">To</span>
												<span class="segment-stop-name"
													>{feature.properties?.to_stop_name ?? '—'}</span
												>
											</div>
										</td>
										<td>
											{#if feature.properties?.bunching_rate === undefined || feature.properties?.bunching_rate === null}
												—
											{:else}
												<span
													class={`risk-pill ${getRiskLevel(feature.properties?.bunching_rate)}`}
												>
													{formatPercent(feature.properties?.bunching_rate)}
												</span>
											{/if}
										</td>
										<td>{formatHeadways(feature.properties?.total_headways)}</td>
									</tr>
								{/each}
							{/if}
						</tbody>
					</table>
				</div>
			</div>
			<RouteMap segmentsGeoJson={segments} selectedTimeBucket={bucket} />
		</div>

		<div class="section-gap">
			<BunchingChart data={stats?.buckets ?? []} title={chartTitle} />
		</div>
	</div>
</section>
