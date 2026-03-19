import { describe, expect, it } from 'vitest'
import {
	_buildHourlyBucketsQuery,
	_buildSummaryQuery
} from '../src/routes/api/routes/[routeId]/stats/+server'

describe('route stats hourly query builder', () => {
	it('builds a 24-hour query using America/Chicago local hour extraction', () => {
		const { sql, paramsList } = _buildHourlyBucketsQuery({
			routeId: '22',
			serviceId: null
		})

		expect(paramsList).toEqual(['22'])
		expect(sql).toContain('SELECT generate_series(0, 23)::int AS hour_of_day')
		expect(sql).toContain('rhs.hour_of_day')
		expect(sql).toContain('FROM route_hourly_bunching_stats AS rhs')
		expect(sql).toContain('ORDER BY h.hour_of_day')
	})

	it('applies weekday service filters against he.service_id without changing param order', () => {
		const { sql, paramsList } = _buildHourlyBucketsQuery({
			routeId: '22',
			serviceId: 'weekday'
		})

		expect(paramsList).toEqual(['22'])
		expect(sql).toContain('gc.service_id = rhs.service_id')
		expect(sql).toContain('gc.monday = 1')
	})

	it('applies saturday and sunday service filters against rhs.service_id', () => {
		const saturdayQuery = _buildHourlyBucketsQuery({
			routeId: '22',
			serviceId: 'saturday'
		})
		const sundayQuery = _buildHourlyBucketsQuery({
			routeId: '22',
			serviceId: 'sunday'
		})

		expect(saturdayQuery.paramsList).toEqual(['22'])
		expect(sundayQuery.paramsList).toEqual(['22'])
		expect(saturdayQuery.sql).toContain('gc.service_id = rhs.service_id')
		expect(saturdayQuery.sql).toContain('gc.saturday = 1')
		expect(sundayQuery.sql).toContain('gc.service_id = rhs.service_id')
		expect(sundayQuery.sql).toContain('gc.sunday = 1')
	})

	it('adds custom service IDs as the second positional parameter', () => {
		const { sql, paramsList } = _buildHourlyBucketsQuery({
			routeId: '22',
			serviceId: 'special_service'
		})

		expect(paramsList).toEqual(['22', 'special_service'])
		expect(sql).toContain('rhs.service_id = $2')
	})
})

describe('route stats summary query builder', () => {
	it('builds the summary query without a bucket filter by default', () => {
		const { sql, paramsList } = _buildSummaryQuery({
			routeId: '22',
			serviceId: null,
			bucket: null
		})

		expect(paramsList).toEqual(['22'])
		expect(sql).toContain('SUM(rbs.total_headways)::int AS total_headways')
		expect(sql).toContain('AVG(rbs.median_actual_headway) AS median_actual_headway')
		expect(sql).toContain(
			'percentile_cont(0.5) within group (order by he.scheduled_headway_min)'
		)
		expect(sql).not.toContain('rbs.time_of_day_bucket =')
	})

	it('adds the bucket filter to both aggregate and median scheduled headway queries', () => {
		const { sql, paramsList } = _buildSummaryQuery({
			routeId: '22',
			serviceId: null,
			bucket: 'AM_peak'
		})

		expect(paramsList).toEqual(['22', 'AM_peak'])
		expect(sql).toContain('rbs.time_of_day_bucket = $2')
		expect(sql).toContain(
			'coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time)) = $2'
		)
	})

	it('applies weekday service filters without changing parameter order', () => {
		const { sql, paramsList } = _buildSummaryQuery({
			routeId: '22',
			serviceId: 'weekday',
			bucket: 'AM_peak'
		})

		expect(paramsList).toEqual(['22', 'AM_peak'])
		expect(sql).toContain('gc.service_id = rbs.service_id')
		expect(sql).toContain('gc.service_id = he.service_id')
		expect(sql).toContain('gc.monday = 1')
		expect(sql).toContain('rbs.time_of_day_bucket = $2')
		expect(sql).toContain(
			'coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time)) = $2'
		)
	})

	it('adds a custom service ID before the bucket parameter', () => {
		const { sql, paramsList } = _buildSummaryQuery({
			routeId: '22',
			serviceId: 'special_service',
			bucket: 'PM_peak'
		})

		expect(paramsList).toEqual(['22', 'special_service', 'PM_peak'])
		expect(sql).toContain('rbs.service_id = $2')
		expect(sql).toContain('he.service_id = $2')
		expect(sql).toContain('rbs.time_of_day_bucket = $3')
		expect(sql).toContain(
			'coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time)) = $3'
		)
	})
})
