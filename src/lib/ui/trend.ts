export type TrendPoint = { date: string; value: number | null }

const mean = (values: number[]): number =>
	values.reduce((sum, v) => sum + v, 0) / values.length

/**
 * Week-over-week change: mean of the most recent 7 valued days minus the mean of
 * the 7 days before that.  Returns null until there are at least two valued days
 * in the prior week to compare against, so a brand-new series shows no delta.
 */
export const computeWeekOverWeekDelta = (points: TrendPoint[]): number | null => {
	const valued = points
		.filter((p): p is { date: string; value: number } => p.value !== null)
		.map((p) => p.value)

	if (valued.length < 2) return null

	const last7 = valued.slice(-7)
	const prev7 = valued.slice(-14, -7)
	if (prev7.length === 0) return null

	return mean(last7) - mean(prev7)
}

/**
 * Map trend points to x/y coordinates within a [width × height] box.  Nulls are
 * dropped (gaps), and the y-axis is scaled to the observed value range with a
 * small headroom so a flat series still renders mid-box.
 */
export const buildSparklinePoints = (
	points: TrendPoint[],
	width: number,
	height: number
): { x: number; y: number }[] => {
	const valued = points
		.map((p, i) => ({ i, value: p.value }))
		.filter((p): p is { i: number; value: number } => p.value !== null)

	if (valued.length === 0) return []

	const values = valued.map((p) => p.value)
	const min = Math.min(...values)
	const max = Math.max(...values)
	const span = max - min || 1
	const n = points.length

	return valued.map(({ i, value }) => ({
		x: n <= 1 ? 0 : (i / (n - 1)) * width,
		y: height - ((value - min) / span) * height
	}))
}
