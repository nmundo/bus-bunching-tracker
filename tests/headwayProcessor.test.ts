import { describe, expect, it } from 'vitest'
import { _buildHeadwaysInsertSql } from '../worker/src/headwayProcessor'

describe('headway segment matching query', () => {
	it('uses route+stop lateral segment matching with direction preference', () => {
		const sql = _buildHeadwaysInsertSql()

		expect(sql).toContain('left join lateral')
		expect(sql).toContain('where s.route_id = o.route_id')
		expect(sql).toContain('and s.to_stop_id = o.stop_id')
		expect(sql).toContain('when s.direction_id = o.direction_id then 0')
		expect(sql).toContain('when s.direction_id is null then 2')
		expect(sql).toContain('limit 1')
	})
})
