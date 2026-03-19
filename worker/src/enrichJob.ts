import { fileURLToPath } from 'node:url'
import { query } from './db'

const DEFAULT_BATCH_SIZE = 10000
const DEFAULT_MAX_BATCHES = 120

export const _buildEnrichBatchSql = () =>
	'select enrich_headways_batch_safe($1::integer) as inserted_rows'

export const runEnrich = async ({
	batchSize = DEFAULT_BATCH_SIZE,
	maxBatches = DEFAULT_MAX_BATCHES
}: {
	batchSize?: number
	maxBatches?: number
} = {}) => {
	let totalInserted = 0

	for (let batch = 0; batch < maxBatches; batch += 1) {
		const result = await query<{ inserted_rows: number | string | null }>(_buildEnrichBatchSql(), [
			batchSize
		])
		const insertedRows = Number(result.rows[0]?.inserted_rows ?? 0)
		totalInserted += insertedRows
		if (insertedRows === 0) break
	}

	await query('select refresh_route_bunching_stats(30)')
	await query('select refresh_segment_bunching_stats(30)')
	await query('select refresh_route_hourly_bunching_stats(30)')
	console.log(`Enrichment and stats refresh complete (${totalInserted} rows inserted)`)
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runEnrich().catch((error) => {
		console.error('Enrichment job failed', error)
		process.exit(1)
	})
}
