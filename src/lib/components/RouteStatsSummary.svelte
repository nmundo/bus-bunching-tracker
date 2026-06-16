<script lang="ts">
	type Summary = {
		bunching_rate: number | null
		mean_scheduled_headway: number | null
		mean_actual_headway: number | null
		excess_wait_min?: number | null
		headway_cv?: number | null
	}

	let { summary }: { summary: Summary } = $props()

	const EWT_FREQUENT_HEADWAY_MAX = 12

	const formatPercent = (value: number | null) =>
		value === null ? '—' : `${(value * 100).toFixed(1)}%`
	const formatNumber = (value: number | null | undefined) =>
		value === null || value === undefined ? '—' : value.toFixed(2)
	const excessWaitLabel = (() => {
		const ewt = summary.excess_wait_min
		const sched = summary.mean_scheduled_headway
		if (ewt === null || ewt === undefined) return '—'
		if (sched === null || sched === undefined || sched > EWT_FREQUENT_HEADWAY_MAX) return 'n/a'
		// Floored at 0: negative excess wait (better than schedule) reads oddly.
		return `+${Math.max(0, ewt).toFixed(1)} min`
	})()
</script>

<div class="kpi-grid">
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
		<p class="meta-line">Mean scheduled headway (min)</p>
		<h3>{formatNumber(summary.mean_scheduled_headway)}</h3>
		<p>Typical planned spacing between buses on this route</p>
	</div>
	<div class="stat-card">
		<p class="meta-line">Mean actual headway (min)</p>
		<h3>{formatNumber(summary.mean_actual_headway)}</h3>
		<p>Typical spacing between buses on this route</p>
	</div>
</div>
