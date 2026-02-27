import { describe, expect, it } from 'vitest'
import { getSegmentHeatColor, getSegmentHeatLevel } from '../src/lib/ui/segmentHeatmap'

describe('segment heatmap classification', () => {
	it('classifies low bunching rates as green', () => {
		expect(getSegmentHeatLevel(0.05)).toBe('low')
		expect(getSegmentHeatColor(0.05)).toBe('#18864b')
	})

	it('classifies medium bunching rates as yellow', () => {
		expect(getSegmentHeatLevel(0.15)).toBe('medium')
		expect(getSegmentHeatColor(0.15)).toBe('#f0b429')
	})

	it('classifies high bunching rates as red', () => {
		expect(getSegmentHeatLevel(0.25)).toBe('high')
		expect(getSegmentHeatColor(0.25)).toBe('#c4321a')
	})

	it('classifies missing bunching rates as unknown gray', () => {
		expect(getSegmentHeatLevel(null)).toBe('unknown')
		expect(getSegmentHeatColor(null)).toBe('#98a2b3')
	})
})
