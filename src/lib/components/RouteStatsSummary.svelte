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
</script>

<div class="grid two">
	<div class="panel">
		<small class="mono">Bunching rate</small>
		<h2>{formatPercent(summary.bunching_rate)}</h2>
	</div>
	<div class="panel">
		<small class="mono">Total headways</small>
		<h2>{summary.total_headways ?? '—'}</h2>
	</div>
	<div class="panel">
		<small class="mono">Avg headway ratio</small>
		<h2>{formatNumber(summary.avg_hw_ratio)}</h2>
	</div>
	<div class="panel">
		<small class="mono">Median actual headway (min)</small>
		<h2>{formatNumber(summary.median_actual_headway)}</h2>
	</div>
</div>
