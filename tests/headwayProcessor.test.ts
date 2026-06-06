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

	it('keeps distinct vehicles that share an arrival_time instead of collapsing them', () => {
		const sql = _buildHeadwaysInsertSql()

		// Only exact duplicates are collapsed; two different vehicles at the same
		// timestamp must survive so the bunched pair produces a near-zero headway.
		expect(sql).toContain('select distinct route_id, direction_id, stop_id, vid, arrival_time')
		expect(sql).not.toContain('row_number() over')
		// Deterministic tie-break ordering on vid.
		expect(sql).toContain('order by arrival_time, vid')
		// Zero-minute headways are allowed; self-pairs are excluded.
		expect(sql).toContain('o.arrival_time >= o.prev_time')
		expect(sql).toContain('o.prev_vid <> o.vid')
	})
})
