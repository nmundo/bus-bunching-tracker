<script lang="ts">
	import type { BucketStat } from '$lib/types/frontend'

	let { data = [] }: { data?: BucketStat[] } = $props()

	const maxRate = $derived(Math.max(0.1, ...data.map((d) => d.bunching_rate ?? 0)))
	const guides = [0, 0.25, 0.5, 0.75, 1]
	const formatBucketLabel = (value: string) => value.replace('_', ' ')

	const worstBucket = $derived.by(() => {
		let worst: BucketStat | null = null

		for (const bucket of data) {
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
			<p class="meta-line">Trend by time bucket</p>
			<h3>Bunching rate by time of day</h3>
		</div>
		{#if worstBucket?.bunching_rate !== null && worstBucket}
			<small class="mono">
				Peak risk: {formatBucketLabel(worstBucket.time_of_day_bucket)} ({(
					worstBucket.bunching_rate * 100
				).toFixed(1)}%)
			</small>
		{/if}
	</div>
	<div class="chart-shell">
		<svg
			viewBox="0 0 640 260"
			width="100%"
			height="260"
			role="img"
			aria-label="Bunching rate by time of day"
		>
			{#if data.length === 0}
				<text x="28" y="132" class="chart-muted">No stats yet.</text>
			{:else}
				{#each guides as guide (guide)}
					{@const y = 210 - guide * 150}
					<line x1="70" y1={y} x2="600" y2={y} class="chart-grid" />
					<text x="16" y={y + 4} class="chart-axis">{(guide * maxRate * 100).toFixed(0)}%</text>
				{/each}
				{#each data as item, index (item.time_of_day_bucket)}
					{@const rate = item.bunching_rate ?? 0}
					{@const barWidth = 78}
					{@const gap = 24}
					{@const x = 82 + index * (barWidth + gap)}
					{@const barHeight = (rate / maxRate) * 150}
					{@const y = 210 - barHeight}
					{@const highlight = item.time_of_day_bucket === worstBucket?.time_of_day_bucket}
					<rect
						{x}
						{y}
						width={barWidth}
						height={barHeight}
						rx="8"
						class={highlight ? 'chart-bar chart-bar-high' : 'chart-bar'}
					/>
					<text x={x + barWidth / 2} y="228" text-anchor="middle" class="chart-axis">
						{formatBucketLabel(item.time_of_day_bucket)}
					</text>
					<text
						x={x + barWidth / 2}
						y={Math.max(y - 8, 34)}
						text-anchor="middle"
						class={highlight ? 'chart-value chart-value-high' : 'chart-value'}
					>
						{(rate * 100).toFixed(1)}%
					</text>
				{/each}
			{/if}
		</svg>
	</div>
</div>
