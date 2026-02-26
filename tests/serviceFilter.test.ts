import { describe, expect, it } from 'vitest'
import { appendServiceFilter } from '../src/lib/server/serviceFilter'

describe('appendServiceFilter', () => {
	it('does not append any filter for empty value', () => {
		const filters: string[] = []
		const params: unknown[] = []

		appendServiceFilter({
			serviceId: '',
			serviceIdColumn: 'rbs.service_id',
			filters,
			params
		})

		expect(filters).toHaveLength(0)
		expect(params).toHaveLength(0)
	})

	it('maps weekday to calendar weekday flags', () => {
		const filters: string[] = []
		const params: unknown[] = []

		appendServiceFilter({
			serviceId: 'weekday',
			serviceIdColumn: 'rbs.service_id',
			filters,
			params
		})

		expect(filters).toHaveLength(1)
		expect(filters[0]).toContain('FROM gtfs_calendar gc')
		expect(filters[0]).toContain('gc.service_id = rbs.service_id')
		expect(filters[0]).toContain('gc.monday = 1')
		expect(filters[0]).toContain('gc.tuesday = 1')
		expect(filters[0]).toContain('gc.wednesday = 1')
		expect(filters[0]).toContain('gc.thursday = 1')
		expect(filters[0]).toContain('gc.friday = 1')
		expect(params).toHaveLength(0)
	})

	it('maps saturday to calendar saturday flag', () => {
		const filters: string[] = []
		const params: unknown[] = []

		appendServiceFilter({
			serviceId: 'saturday',
			serviceIdColumn: 'sbs.service_id',
			filters,
			params
		})

		expect(filters).toHaveLength(1)
		expect(filters[0]).toContain('gc.service_id = sbs.service_id')
		expect(filters[0]).toContain('gc.saturday = 1')
		expect(filters[0]).not.toContain('gc.sunday = 1')
		expect(params).toHaveLength(0)
	})

	it('maps sunday to calendar sunday flag', () => {
		const filters: string[] = []
		const params: unknown[] = []

		appendServiceFilter({
			serviceId: 'sunday',
			serviceIdColumn: 'sbs.service_id',
			filters,
			params
		})

		expect(filters).toHaveLength(1)
		expect(filters[0]).toContain('gc.service_id = sbs.service_id')
		expect(filters[0]).toContain('gc.sunday = 1')
		expect(filters[0]).not.toContain('gc.saturday = 1')
		expect(params).toHaveLength(0)
	})

	it('treats a raw service id as an exact match', () => {
		const filters: string[] = ['route_id = $1']
		const params: unknown[] = ['22']

		appendServiceFilter({
			serviceId: '67701',
			serviceIdColumn: 'rbs.service_id',
			filters,
			params
		})

		expect(filters).toEqual(['route_id = $1', 'rbs.service_id = $2'])
		expect(params).toEqual(['22', '67701'])
	})

	it('treats unknown values as exact matches', () => {
		const filters: string[] = []
		const params: unknown[] = []

		appendServiceFilter({
			serviceId: 'WKDY',
			serviceIdColumn: 'rbs.service_id',
			filters,
			params
		})

		expect(filters).toEqual(['rbs.service_id = $1'])
		expect(params).toEqual(['WKDY'])
	})
})
