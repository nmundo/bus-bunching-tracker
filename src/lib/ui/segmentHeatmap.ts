export const SEGMENT_HEAT_THRESHOLDS = {
	medium: 0.1,
	high: 0.2
} as const

export const SEGMENT_HEAT_COLORS = {
	low: '#18864b',
	medium: '#f0b429',
	high: '#c4321a',
	unknown: '#98a2b3'
} as const

export type SegmentHeatLevel = 'high' | 'medium' | 'low' | 'unknown'

export const getSegmentHeatLevel = (rate: number | null | undefined): SegmentHeatLevel => {
	if (rate === null || rate === undefined) {
		return 'unknown'
	}
	if (rate >= SEGMENT_HEAT_THRESHOLDS.high) {
		return 'high'
	}
	if (rate >= SEGMENT_HEAT_THRESHOLDS.medium) {
		return 'medium'
	}
	return 'low'
}

export const getSegmentHeatColor = (rate: number | null | undefined): string =>
	SEGMENT_HEAT_COLORS[getSegmentHeatLevel(rate)]
