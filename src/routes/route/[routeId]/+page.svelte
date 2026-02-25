<script lang="ts">
	import BunchingChart from '$components/BunchingChart.svelte'
	import RouteMap from '$components/RouteMap.svelte'
	import RouteStatsSummary from '$components/RouteStatsSummary.svelte'

	export let data: {
		routeId: string
		stats: {
			route: {
				route_id: string
				route_short_name: string
				route_long_name: string | null
			} | null
			summary: {
				bunching_rate: number | null
				total_headways: number | null
				avg_hw_ratio: number | null
				median_actual_headway: number | null
			} | null
			buckets: { time_of_day_bucket: string; bunching_rate: number | null }[]
		} | null
		segments: GeoJSON.FeatureCollection<GeoJSON.LineString> | null
		serviceId: string
		bucket: string
	}

	let serviceId = data.serviceId
	let bucket = data.bucket
	let stats = data.stats
	let segments = data.segments
	let loading = false

	const timeBuckets = ['AM_peak', 'Midday', 'PM_peak', 'Evening', 'Night']

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

	const worstSegments = () => {
		if (!segments) return []
		return [...segments.features]
			.sort((a, b) => (b.properties?.bunching_rate ?? 0) - (a.properties?.bunching_rate ?? 0))
			.slice(0, 8)
	}
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
				Service ID
				<input placeholder="WKDY" bind:value={serviceId} />
			</label>
			<label>
				Time bucket
				<select bind:value={bucket}>
					{#each timeBuckets as option}
						<option value={option}>{option}</option>
					{/each}
				</select>
			</label>
			<button on:click={refresh} disabled={loading}>Refresh</button>
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
					{#each worstSegments() as feature}
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
