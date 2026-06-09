import { describe, expect, it } from 'vitest'
import {
	_buildStopArrivalsInsertSql,
	_flattenStopArrivalRows,
	interpolateCrossingTime,
	stopIndexForPdist
} from '../worker/src/arrivalsProcessor'

const stop = (seq: number, distance_feet: number) => ({
	seq,
	stpid: `s${seq}`,
	gtfs_stop_id: `g${seq}`,
	distance_feet
})

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

describe('interpolateCrossingTime', () => {
	const prevTimestamp = new Date('2026-01-01T12:00:00Z')
	const currTimestamp = new Date('2026-01-01T12:00:40Z') // 40s later

	it('interpolates a stop midway between the two pdist samples', () => {
		const result = interpolateCrossingTime({
			stopDistance: 1500,
			prevPdist: 1000,
			prevTimestamp,
			currPdist: 2000,
			currTimestamp
		})
		// Halfway in distance -> halfway in time (20s past prev).
		expect(result.getTime()).toBe(new Date('2026-01-01T12:00:20Z').getTime())
	})

	it('clamps to the prior timestamp when the stop sits at/below prevPdist', () => {
		const result = interpolateCrossingTime({
			stopDistance: 900,
			prevPdist: 1000,
			prevTimestamp,
			currPdist: 2000,
			currTimestamp
		})
		expect(result.getTime()).toBe(prevTimestamp.getTime())
	})

	it('clamps to the current timestamp when the stop sits at/above currPdist', () => {
		const result = interpolateCrossingTime({
			stopDistance: 2500,
			prevPdist: 1000,
			prevTimestamp,
			currPdist: 2000,
			currTimestamp
		})
		expect(result.getTime()).toBe(currTimestamp.getTime())
	})

	it('falls back to the current timestamp when there is no usable prior sample', () => {
		const result = interpolateCrossingTime({
			stopDistance: 1500,
			prevPdist: 1000,
			prevTimestamp: new Date(0), // seed sentinel
			currPdist: 2000,
			currTimestamp
		})
		expect(result.getTime()).toBe(currTimestamp.getTime())
	})

	it('falls back to the current timestamp when the vehicle did not move forward', () => {
		const result = interpolateCrossingTime({
			stopDistance: 1500,
			prevPdist: 2000,
			prevTimestamp,
			currPdist: 2000,
			currTimestamp
		})
		expect(result.getTime()).toBe(currTimestamp.getTime())
	})
})

describe('stopIndexForPdist', () => {
	const stops = [stop(0, 0), stop(1, 1000), stop(2, 2000), stop(3, 3000)]

	it('returns the index of the last stop already passed', () => {
		// Anchors a first-seen vehicle so the while loop emits no arrivals this poll;
		// without it the vehicle would backfill stops 0..current at one timestamp.
		expect(stopIndexForPdist(stops, 2500)).toBe(2)
	})

	it('includes a stop sitting exactly at the current pdist', () => {
		expect(stopIndexForPdist(stops, 2000)).toBe(2)
	})

	it('returns -1 only when the vehicle is before the first stop', () => {
		expect(stopIndexForPdist(stops, -1)).toBe(-1)
	})

	it('returns the final index when the vehicle is at or past the last stop', () => {
		expect(stopIndexForPdist(stops, 9999)).toBe(3)
	})

	it('tolerates non-monotonic distances by scanning the whole pattern', () => {
		const wavy = [stop(0, 0), stop(1, 3000), stop(2, 1000), stop(3, 2000)]
		expect(stopIndexForPdist(wavy, 2500)).toBe(3)
	})
})
