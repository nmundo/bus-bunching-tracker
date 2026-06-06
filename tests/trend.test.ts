import { describe, expect, it } from 'vitest'
import { buildSparklinePoints, computeWeekOverWeekDelta, type TrendPoint } from '../src/lib/ui/trend'

const days = (values: (number | null)[]): TrendPoint[] =>
	values.map((value, i) => ({ date: `2026-06-${String(i + 1).padStart(2, '0')}`, value }))

describe('computeWeekOverWeekDelta', () => {
	it('returns null until there is a prior week to compare', () => {
		expect(computeWeekOverWeekDelta(days([0.1]))).toBeNull()
		expect(computeWeekOverWeekDelta(days([]))).toBeNull()
	})

	it('compares the mean of the last 7 valued days against the previous 7', () => {
		// prev week mean 0.10, last week mean 0.20 -> +0.10
		const points = days([
			0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
			0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2
		])
		expect(computeWeekOverWeekDelta(points)).toBeCloseTo(0.1, 6)
	})

	it('ignores null days when forming the windows', () => {
		// nulls are dropped first; valued = [0.1, 0.1] + seven 0.2s (length 9)
		const points = days([0.1, null, 0.1, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2])
		// last7 = seven 0.2s (mean 0.2); prev7 = the two 0.1s (mean 0.1) -> +0.10
		expect(computeWeekOverWeekDelta(points)).toBeCloseTo(0.1, 6)
	})
})

describe('buildSparklinePoints', () => {
	it('drops null gaps and scales y to the value range', () => {
		const coords = buildSparklinePoints(days([0, null, 1]), 100, 10)
		expect(coords).toHaveLength(2)
		// min (0) maps to bottom (y=height), max (1) maps to top (y=0)
		expect(coords[0]).toEqual({ x: 0, y: 10 })
		expect(coords[1]).toEqual({ x: 100, y: 0 })
	})

	it('returns no points for an all-null series', () => {
		expect(buildSparklinePoints(days([null, null]), 100, 10)).toEqual([])
	})
})
