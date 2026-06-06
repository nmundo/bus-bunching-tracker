<script lang="ts">
	import { buildSparklinePoints, computeWeekOverWeekDelta, type TrendPoint } from '$lib/ui/trend'

	let {
		points = [],
		label = 'Trend',
		// How to read a delta: for most metrics lower is better (less bunching).
		lowerIsBetter = true,
		format = (v: number) => v.toFixed(2),
		width = 160,
		height = 36
	}: {
		points?: TrendPoint[]
		label?: string
		lowerIsBetter?: boolean
		format?: (v: number) => string
		width?: number
		height?: number
	} = $props()

	const valuedCount = $derived(points.filter((p) => p.value !== null).length)
	const coords = $derived(buildSparklinePoints(points, width, height))
	const polyline = $derived(coords.map((c) => `${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(' '))
	const delta = $derived(computeWeekOverWeekDelta(points))

	const deltaTone = $derived.by(() => {
		if (delta === null || delta === 0) return 'flat'
		const worsening = lowerIsBetter ? delta > 0 : delta < 0
		return worsening ? 'worse' : 'better'
	})
	const deltaText = $derived.by(() => {
		if (delta === null) return ''
		const arrow = delta > 0 ? '▲' : delta < 0 ? '▼' : '→'
		return `${arrow} ${format(Math.abs(delta))} vs prior week`
	})
</script>

<div class="trend">
	{#if valuedCount < 2}
		<span class="trend-empty">Not enough history yet — collecting daily data.</span>
	{:else}
		<svg
			class="trend-spark"
			viewBox={`0 0 ${width} ${height}`}
			width={width}
			height={height}
			role="img"
			aria-label={`${label} sparkline over ${valuedCount} days`}
			preserveAspectRatio="none"
		>
			<polyline points={polyline} fill="none" stroke="currentColor" stroke-width="1.5" />
			{#if coords.length > 0}
				<circle cx={coords[coords.length - 1].x} cy={coords[coords.length - 1].y} r="2" />
			{/if}
		</svg>
		<span class={`trend-delta ${deltaTone}`}>{deltaText}</span>
	{/if}
</div>

<style>
	.trend {
		display: flex;
		align-items: center;
		gap: 10px;
	}
	.trend-spark {
		color: var(--chart-bar, #6c7684);
		flex: 0 0 auto;
	}
	.trend-spark circle {
		fill: currentColor;
	}
	.trend-empty {
		font-size: 12px;
		color: var(--text-muted);
	}
	.trend-delta {
		font-size: 12px;
		font-weight: 600;
		white-space: nowrap;
	}
	.trend-delta.worse {
		color: var(--risk-high, #b42318);
	}
	.trend-delta.better {
		color: var(--risk-low, #166534);
	}
	.trend-delta.flat {
		color: var(--text-muted);
	}
</style>
