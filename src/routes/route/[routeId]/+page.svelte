<script lang="ts">
	import { browser } from '$app/environment'
	import BunchingChart from '$components/BunchingChart.svelte'
	import RouteMap from '$components/RouteMap.svelte'
	import RouteStatsSummary from '$components/RouteStatsSummary.svelte'
	import { withRouteDetailFilterParams } from '$lib/ui/routeDetailUrl'
	import { classifyRisk, type RiskLevel } from '$lib/ui/networkMetrics'
	import type { PageData } from './$types'

	type Props = {
		data: PageData
	}

	let { data }: Props = $props()

	let serviceId = $derived(data.serviceId)
	let bucket = $derived(data.bucket)
	let directionId = $state(data.directionId ?? '')
	let stats = $derived<PageData['stats']>(data.stats)
	let segments = $derived<PageData['segments']>(data.segments)
	let loading = $state(false)

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
		const statsParams = new URLSearchParams()
		if (serviceId) statsParams.set('service_id', serviceId)
		if (bucket) statsParams.set('time_of_day_bucket', bucket)
		if (directionId !== '') statsParams.set('direction_id', directionId)

		const segmentsParams = new URLSearchParams()
		if (serviceId) segmentsParams.set('service_id', serviceId)
		if (bucket) segmentsParams.set('time_of_day_bucket', bucket)
		if (directionId !== '') segmentsParams.set('direction_id', directionId)

		const [statsRes, segmentsRes] = await Promise.all([
			fetch(`/api/routes/${data.routeId}/stats?${statsParams.toString()}`),
			fetch(`/api/routes/${data.routeId}/segments?${segmentsParams.toString()}`)
		])

		stats = statsRes.ok ? await statsRes.json() : null
		segments = segmentsRes.ok ? await segmentsRes.json() : null
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
		<a class="back-link" href="/">← Back to network overview</a>

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
					<option value="0">Direction 0</option>
					<option value="1">Direction 1</option>
				</select>
			</label>
		</div>
	</div>

	{#if stats?.summary}
		<RouteStatsSummary summary={{ ...stats.summary, route: stats.route }} />
	{/if}

	<div class="grid two">
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
							<th>Total headways</th>
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
											<span class="segment-stop-name">{feature.properties?.from_stop_name ?? '—'}</span>
										</div>
										<div class="segment-stop-row">
											<span class="segment-stop-label">To</span>
											<span class="segment-stop-name">{feature.properties?.to_stop_name ?? '—'}</span>
										</div>
									</td>
									<td>
										{#if feature.properties?.bunching_rate === undefined || feature.properties?.bunching_rate === null}
											—
										{:else}
											<span class={`risk-pill ${getRiskLevel(feature.properties?.bunching_rate)}`}>
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

	<BunchingChart data={stats?.buckets ?? []} />
</section>
