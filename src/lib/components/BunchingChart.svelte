<script lang="ts">
	import type { BucketStat } from '$lib/types/frontend'

	let { data = [] }: { data?: BucketStat[] } = $props()

	const orderedData = $derived.by(() => [...data].sort((a, b) => a.hour_of_day - b.hour_of_day))
	const maxRate = $derived.by(() => {
		const rates = orderedData.flatMap((bucket) =>
			bucket.bunching_rate === null ? [] : [bucket.bunching_rate]
		)
		return Math.max(0.1, ...rates)
	})
	const guides = [0, 0.25, 0.5, 0.75, 1]
	const chartLeft = 70
	const chartRight = 26
	const chartTop = 26
	const plotHeight = 156
	const chartBottom = 52
	const barWidth = 24
	const gap = 14

	const viewWidth = $derived(
		chartLeft +
			chartRight +
			orderedData.length * barWidth +
			Math.max(orderedData.length - 1, 0) * gap
	)
	const viewHeight = chartTop + plotHeight + chartBottom

	const formatHourLabel = (hourOfDay: number) => {
		const normalizedHour = ((hourOfDay % 24) + 24) % 24
		const displayHour = normalizedHour % 12 === 0 ? 12 : normalizedHour % 12
		const suffix = normalizedHour < 12 ? 'a' : 'p'
		return `${displayHour}${suffix}`
	}

	const worstBucket = $derived.by(() => {
		let worst: BucketStat | null = null

		for (const bucket of orderedData) {
			if (bucket.bunching_rate === null) {
				continue
			}
			if (!worst || (worst.bunching_rate ?? -1) < bucket.bunching_rate) {
				worst = bucket
			}
		}

		return worst
	})
</script>

<div class="panel visual-panel">
	<div class="section-head">
		<div>
			<p class="meta-line">Trend by hour</p>
			<h3>Bunching rate by hour of day</h3>
		</div>
		{#if worstBucket?.bunching_rate !== null && worstBucket}
			<small class="mono">
				Peak risk: {formatHourLabel(worstBucket.hour_of_day)} ({(
					worstBucket.bunching_rate * 100
				).toFixed(1)}%)
			</small>
		{/if}
	</div>
	<div class="chart-shell">
		<svg
			viewBox={`0 0 ${viewWidth} ${viewHeight}`}
			width="100%"
			height={viewHeight}
			role="img"
			aria-label="Bunching rate by hour of day"
		>
			{#if orderedData.length === 0}
				<text x="28" y="132" class="chart-muted">No stats yet.</text>
			{:else}
				{#each guides as guide (guide)}
					{@const y = chartTop + plotHeight - guide * plotHeight}
					<line x1={chartLeft} y1={y} x2={viewWidth - chartRight} y2={y} class="chart-grid" />
					<text x="16" y={y + 4} class="chart-axis">{(guide * maxRate * 100).toFixed(0)}%</text>
				{/each}
				{#each orderedData as item, index (item.hour_of_day)}
					{@const rate = item.bunching_rate ?? 0}
					{@const x = chartLeft + index * (barWidth + gap)}
					{@const hasData = item.total_headways > 0 && item.bunching_rate !== null}
					{@const barHeight = hasData ? (rate / maxRate) * plotHeight : plotHeight}
					{@const y = chartTop + plotHeight - barHeight}
					{@const highlight = item.hour_of_day === worstBucket?.hour_of_day && hasData}
					{#if hasData}
						<rect
							{x}
							{y}
							width={barWidth}
							height={barHeight}
							rx="6"
							class={highlight ? 'chart-bar chart-bar-high' : 'chart-bar'}
						>
							<title>{formatHourLabel(item.hour_of_day)}: {(rate * 100).toFixed(1)}%</title>
						</rect>
					{:else}
						<rect
							{x}
							y={chartTop}
							width={barWidth}
							height={plotHeight}
							rx="6"
							class="chart-bar-empty"
						>
							<title>{formatHourLabel(item.hour_of_day)}: no data</title>
						</rect>
					{/if}
					<text
						x={x + barWidth / 2}
						y={chartTop + plotHeight + 18}
						text-anchor="middle"
						class="chart-axis chart-axis-tight"
					>
						{formatHourLabel(item.hour_of_day)}
					</text>
					{#if hasData}
						<text
							x={x + barWidth / 2}
							y={Math.max(y - 8, chartTop - 6)}
							text-anchor="middle"
							class={highlight ? 'chart-value chart-value-high' : 'chart-value'}
						>
							{(rate * 100).toFixed(1)}%
						</text>
					{/if}
				{/each}
			{/if}
		</svg>
	</div>
</div>
