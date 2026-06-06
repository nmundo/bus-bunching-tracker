import { describe, expect, it } from 'vitest'
import {
	_buildStopArrivalsInsertSql,
	_flattenStopArrivalRows
} from '../worker/src/arrivalsProcessor'

describe('arrivals processor batched inserts', () => {
	it('builds multi-row stop arrival inserts with positional parameters', () => {
		const sql = _buildStopArrivalsInsertSql(2)

		expect(sql).toContain('insert into stop_arrivals')
		expect(sql).toContain(
			'(route_id, direction_id, stop_id, vid, rt, pid, arrival_time, pdist_feet)'
		)
		expect(sql).toContain('($1, $2, $3, $4, $5, $6, $7, $8)')
		expect(sql).toContain('($9, $10, $11, $12, $13, $14, $15, $16)')
	})

	it('flattens stop arrival rows in column order', () => {
		const arrivalTime = new Date('2026-01-01T12:00:00Z')

		expect(
			_flattenStopArrivalRows([
				{
					route_id: '8',
					direction_id: 1,
					stop_id: '123',
					vid: 'vehicle-1',
					rt: '8',
					pid: 'pattern-1',
					arrival_time: arrivalTime,
					pdist_feet: 42
				}
			])
		).toEqual(['8', 1, '123', 'vehicle-1', '8', 'pattern-1', arrivalTime, 42])
	})
})
