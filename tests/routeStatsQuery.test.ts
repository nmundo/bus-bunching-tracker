import { describe, expect, it } from 'vitest'
import { _buildHourlyBucketsQuery } from '../src/routes/api/routes/[routeId]/stats/+server'

describe('route stats hourly query builder', () => {
	it('builds a 24-hour query using America/Chicago local hour extraction', () => {
		const { sql, paramsList } = _buildHourlyBucketsQuery({
			routeId: '22',
			serviceId: null
		})

		expect(paramsList).toEqual(['22'])
		expect(sql).toContain('SELECT generate_series(0, 23)::int AS hour_of_day')
		expect(sql).toContain(
			"extract(hour from (he.arrival_time at time zone 'America/Chicago'))::int AS hour_of_day"
		)
		expect(sql).toContain("he.arrival_time >= now() - interval '30 day'")
		expect(sql).toContain('FROM headways_enriched AS he')
		expect(sql).toContain('ORDER BY h.hour_of_day')
	})

	it('applies weekday service filters against he.service_id without changing param order', () => {
		const { sql, paramsList } = _buildHourlyBucketsQuery({
			routeId: '22',
			serviceId: 'weekday'
		})

		expect(paramsList).toEqual(['22'])
		expect(sql).toContain('gc.service_id = he.service_id')
		expect(sql).toContain('gc.monday = 1')
		expect(sql).toContain("he.arrival_time >= now() - interval '30 day'")
	})

	it('applies saturday and sunday service filters against he.service_id', () => {
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
		expect(saturdayQuery.sql).toContain('gc.service_id = he.service_id')
		expect(saturdayQuery.sql).toContain('gc.saturday = 1')
		expect(sundayQuery.sql).toContain('gc.service_id = he.service_id')
		expect(sundayQuery.sql).toContain('gc.sunday = 1')
	})

	it('adds custom service IDs as the second positional parameter', () => {
		const { sql, paramsList } = _buildHourlyBucketsQuery({
			routeId: '22',
			serviceId: 'special_service'
		})

		expect(paramsList).toEqual(['22', 'special_service'])
		expect(sql).toContain('he.service_id = $2')
	})
})
