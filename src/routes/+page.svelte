<script lang="ts">
	import RouteTable from '$components/RouteTable.svelte'
	import type { RouteStat } from '$lib/types/frontend'
	import type { PageData } from './$types'

	type Props = {
		data: PageData
	}

	let { data }: Props = $props()

	let serviceId = $state('')
	let bucket = $state('')
	let routes = $state<RouteStat[]>([])
	let loading = $state(false)

	const timeBuckets = ['AM_peak', 'Midday', 'PM_peak', 'Evening', 'Night']

	$effect(() => {
		serviceId = data.serviceId
		bucket = data.bucket
		routes = data.routes
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
	<div class="panel">
		<h2>Network overview</h2>
		<p>Scan which routes are struggling with bus bunching and drill into the worst segments.</p>
		<div class="controls">
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

	<div class="panel">
		<RouteTable {routes} />
	</div>
</section>
