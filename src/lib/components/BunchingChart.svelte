<script lang="ts">
	import type { BucketStat } from '$lib/types/frontend'

	let { data = [] }: { data?: BucketStat[] } = $props()

	const maxRate = $derived(Math.max(0.1, ...data.map((d) => d.bunching_rate ?? 0)))
</script>

<div class="panel">
	<h3>Bunching rate by time of day</h3>
	<svg viewBox="0 0 600 220" width="100%" height="220" role="img">
		<rect width="600" height="220" fill="#fff" rx="14" />
		{#if data.length === 0}
			<text x="30" y="110" fill="#6b6259">No stats yet.</text>
		{:else}
			{#each data as item, index (item.time_of_day_bucket)}
				{@const rate = item.bunching_rate ?? 0}
				{@const barHeight = (rate / maxRate) * 140}
				{@const x = 40 + index * 110}
				<rect {x} y={170 - barHeight} width="70" height={barHeight} fill="#d9412f" rx="8" />
				<text {x} y="190" font-size="12" fill="#6b6259">{item.time_of_day_bucket}</text>
				<text {x} y={165 - barHeight} font-size="12" fill="#1e1b16">
					{(rate * 100).toFixed(1)}%
				</text>
			{/each}
		{/if}
	</svg>
</div>
