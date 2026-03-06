import { describe, expect, it } from 'vitest'
import { _buildEnrichBatchSql } from '../worker/src/enrichJob'

describe('enrich job query builder', () => {
	it('uses the batch enrich function with a typed positional parameter', () => {
		expect(_buildEnrichBatchSql()).toBe(
			'select enrich_headways_batch_safe($1::integer) as inserted_rows'
		)
	})
})
