<script lang="ts">
	type Summary = {
		bunching_rate: number | null
		total_headways: number | null
		avg_hw_ratio: number | null
		median_actual_headway: number | null
	}

	let { summary }: { summary: Summary } = $props()

	const formatPercent = (value: number | null) =>
		value === null ? '—' : `${(value * 100).toFixed(1)}%`
	const formatNumber = (value: number | null) => (value === null ? '—' : value.toFixed(2))
	const formatCount = (value: number | null) => (value === null ? '—' : value.toLocaleString())
</script>

<div class="kpi-grid">
	<div class="stat-card">
		<p class="meta-line">Bunching rate</p>
		<h3>{formatPercent(summary.bunching_rate)}</h3>
		<p>Share of observed headways considered bunched</p>
	</div>
	<div class="stat-card">
		<p class="meta-line">Total headways</p>
		<h3>{formatCount(summary.total_headways)}</h3>
		<p>Total measured intervals for this route</p>
	</div>
	<div class="stat-card">
		<p class="meta-line">Avg headway ratio</p>
		<h3>{formatNumber(summary.avg_hw_ratio)}</h3>
		<p>Actual headway divided by scheduled headway</p>
	</div>
	<div class="stat-card">
		<p class="meta-line">Median actual headway (min)</p>
		<h3>{formatNumber(summary.median_actual_headway)}</h3>
		<p>Typical spacing between buses on this route</p>
	</div>
</div>
