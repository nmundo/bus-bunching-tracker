<script lang="ts">
	import BunchingChart from '$components/BunchingChart.svelte'
	import RouteMap from '$components/RouteMap.svelte'
	import RouteStatsSummary from '$components/RouteStatsSummary.svelte'
	import type { PageData } from './$types'

	type Props = {
		data: PageData
	}

	let { data }: Props = $props()

	let serviceId = $state('')
	let bucket = $state('')
	let stats = $state<PageData['stats']>(null)
	let segments = $state<PageData['segments']>(null)
	let loading = $state(false)

	const timeBuckets = ['AM_peak', 'Midday', 'PM_peak', 'Evening', 'Night']

	$effect(() => {
		serviceId = data.serviceId
		bucket = data.bucket
		stats = data.stats
		segments = data.segments
	})

	const refresh = async () => {
		loading = true
		const statsParams = new URLSearchParams()
		if (serviceId) statsParams.set('service_id', serviceId)

		const segmentsParams = new URLSearchParams()
		if (serviceId) segmentsParams.set('service_id', serviceId)
		if (bucket) segmentsParams.set('time_of_day_bucket', bucket)

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
	<div class="panel">
		<div style="display: flex; justify-content: space-between; align-items: baseline; gap: 16px;">
			<div>
				<h2>{stats?.route?.route_short_name ?? data.routeId}</h2>
				<p>{stats?.route?.route_long_name ?? 'Route detail'}</p>
			</div>
			<div class="badge">Route focus</div>
		</div>

		<div class="controls" style="margin-top: 16px;">
			<label>
				Service type
				<select bind:value={serviceId}>
					<option value="">all services</option>
					<option value="weekday">weekday</option>
					<option value="saturday">saturday</option>
					<option value="sunday">sunday</option>
				</select>
			</label>
			<label>
				Time bucket
				<select bind:value={bucket}>
					{#each timeBuckets as option (option)}
						<option value={option}>{option}</option>
					{/each}
				</select>
			</label>
			<button onclick={refresh} disabled={loading}>Refresh</button>
			{#if loading}
				<small class="mono">Loading…</small>
			{/if}
		</div>
	</div>

	{#if stats?.summary}
		<RouteStatsSummary summary={stats.summary} />
	{/if}

	<div class="grid two">
		<BunchingChart data={stats?.buckets ?? []} />
		<RouteMap segmentsGeoJson={segments} selectedTimeBucket={bucket} />
	</div>

	<div class="panel">
		<h3>Worst segments</h3>
		<table class="table">
			<thead>
				<tr>
					<th>From stop</th>
					<th>To stop</th>
					<th>Bunching rate</th>
					<th>Total headways</th>
				</tr>
			</thead>
			<tbody>
				{#if !segments}
					<tr>
						<td colspan="4">No segments loaded.</td>
					</tr>
				{:else}
					{#each worstSegments as feature, index (feature.properties?.segment_id ?? index)}
						<tr>
							<td>{feature.properties?.from_stop_name ?? '—'}</td>
							<td>{feature.properties?.to_stop_name ?? '—'}</td>
							<td>{((feature.properties?.bunching_rate ?? 0) * 100).toFixed(1)}%</td>
							<td>{feature.properties?.total_headways ?? '—'}</td>
						</tr>
					{/each}
				{/if}
			</tbody>
		</table>
	</div>
</section>
