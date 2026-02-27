import { describe, expect, it } from 'vitest'
import {
	_buildSegmentFeatureCollection,
	_buildSegmentsQuery,
	type SegmentRow
} from '../src/routes/api/routes/[routeId]/segments/+server'

describe('segments query builder', () => {
	it('keeps full route selection on segments and applies bucket in stats CTE', () => {
		const { sql, paramsList } = _buildSegmentsQuery({
			routeId: '22',
			serviceId: null,
			bucket: 'AM_peak'
		})

		expect(paramsList).toEqual(['22', 'AM_peak'])
		expect(sql).toContain('WITH filtered_stats AS')
		expect(sql).toContain('FROM segment_bunching_stats sbs')
		expect(sql).toContain('WHERE sbs.route_id = $1')
		expect(sql).toContain('sbs.time_of_day_bucket = $2')
		expect(sql).toContain('LEFT JOIN filtered_stats ON filtered_stats.segment_id = s.id')
		expect(sql).toContain('WHERE s.route_id = $1')
	})

	it('applies weekday service filter within stats CTE without changing params ordering', () => {
		const { sql, paramsList } = _buildSegmentsQuery({
			routeId: '22',
			serviceId: 'weekday',
			bucket: 'PM_peak'
		})

		expect(paramsList).toEqual(['22', 'PM_peak'])
		expect(sql).toContain('gc.service_id = sbs.service_id')
		expect(sql).toContain('gc.monday = 1')
		expect(sql).toContain('sbs.time_of_day_bucket = $2')
		expect(sql).toContain('WHERE s.route_id = $1')
	})
})

describe('segment feature mapping', () => {
	it('returns all rows and marks has_data based on total_headways', () => {
		const rows: SegmentRow[] = [
			{
				segment_id: 'seg-1',
				route_id: '22',
				direction_id: 0,
				from_stop_id: 'stop-a',
				to_stop_id: 'stop-b',
				from_stop_name: 'A',
				to_stop_name: 'B',
				bunching_rate: null,
				total_headways: null,
				bunched_headways: null,
				time_of_day_bucket: null,
				geometry: {
					type: 'LineString',
					coordinates: [
						[-87.6, 41.8],
						[-87.7, 41.9]
					]
				}
			},
			{
				segment_id: 'seg-2',
				route_id: '22',
				direction_id: 1,
				from_stop_id: 'stop-b',
				to_stop_id: 'stop-c',
				from_stop_name: 'B',
				to_stop_name: 'C',
				bunching_rate: 0.25,
				total_headways: 40,
				bunched_headways: 10,
				time_of_day_bucket: 'AM_peak',
				geometry: {
					type: 'LineString',
					coordinates: [
						[-87.7, 41.9],
						[-87.8, 42.0]
					]
				}
			}
		]

		const featureCollection = _buildSegmentFeatureCollection(rows)

		expect(featureCollection.features).toHaveLength(2)
		expect(featureCollection.features[0].properties.has_data).toBe(false)
		expect(featureCollection.features[0].properties.bunching_rate).toBeNull()
		expect(featureCollection.features[1].properties.has_data).toBe(true)
		expect(featureCollection.features[1].properties.bunched_headways).toBe(10)
		expect(featureCollection.features[1].properties.bunching_rate).toBe(0.25)
	})
})
