<script lang="ts">
	type Summary = {
		bunching_rate: number | null
		total_headways: number | null
		median_scheduled_headway: number | null
		median_actual_headway: number | null
		excess_wait_min?: number | null
		headway_cv?: number | null
		route: {
			route_short_name: string | null
			route_long_name: string | null
		}
	}

	let { summary }: { summary: Summary } = $props()

	const EWT_FREQUENT_HEADWAY_MAX = 12

	const formatPercent = (value: number | null) =>
		value === null ? '—' : `${(value * 100).toFixed(1)}%`
	const formatNumber = (value: number | null | undefined) =>
		value === null || value === undefined ? '—' : value.toFixed(2)
	const formatCount = (value: number | null) => (value === null ? '—' : value.toLocaleString())
	const excessWaitLabel = (() => {
		const ewt = summary.excess_wait_min
		const sched = summary.median_scheduled_headway
		if (ewt === null || ewt === undefined) return '—'
		if (sched === null || sched === undefined || sched > EWT_FREQUENT_HEADWAY_MAX) return 'n/a'
		return `${ewt >= 0 ? '+' : ''}${ewt.toFixed(1)} min`
	})()
</script>

<div class="kpi-grid">
	<div class="stat-card">
		<p class="meta-line">Route</p>
		<h3>{summary.route?.route_short_name ?? 'Route detail'}</h3>
		<p>
			{summary.route?.route_long_name ?? 'Route detail'}
		</p>
		<p>Total headways: {formatCount(summary.total_headways)}</p>
	</div>
	<div class="stat-card">
		<p class="meta-line">Bunching rate</p>
		<h3>{formatPercent(summary.bunching_rate)}</h3>
		<p>Share of observed headways considered bunched</p>
	</div>
	<div class="stat-card">
		<p class="meta-line">Excess wait (min)</p>
		<h3>{excessWaitLabel}</h3>
		<p>Extra wait vs. schedule (frequent service only)</p>
	</div>
	<div class="stat-card">
		<p class="meta-line">Headway CV</p>
		<h3>{formatNumber(summary.headway_cv)}</h3>
		<p>Spacing irregularity — 0 is perfectly even</p>
	</div>
	<div class="stat-card">
		<p class="meta-line">Median scheduled headway (min)</p>
		<h3>{formatNumber(summary.median_scheduled_headway)}</h3>
		<p>Typical planned spacing between buses on this route</p>
	</div>
	<div class="stat-card">
		<p class="meta-line">Median actual headway (min)</p>
		<h3>{formatNumber(summary.median_actual_headway)}</h3>
		<p>Typical spacing between buses on this route</p>
	</div>
</div>
