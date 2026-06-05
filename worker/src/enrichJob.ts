import { fileURLToPath } from 'node:url'
import { query, closePool } from './db'

const DEFAULT_BATCH_SIZE = 10000
const DEFAULT_MAX_BATCHES = 120

export const ENRICH_BATCH_SQL = 'select enrich_headways_batch_safe($1::integer) as inserted_rows'

// Kept for backwards-compat with any existing tests that import this name.
export const _buildEnrichBatchSql = () => ENRICH_BATCH_SQL

export const runEnrich = async ({
	batchSize = DEFAULT_BATCH_SIZE,
	maxBatches = DEFAULT_MAX_BATCHES
}: {
	batchSize?: number
	maxBatches?: number
} = {}) => {
	let totalInserted = 0

	for (let batch = 0; batch < maxBatches; batch += 1) {
		const result = await query<{ inserted_rows: number | string | null }>(ENRICH_BATCH_SQL, [
			batchSize
		])
		const insertedRows = Number(result.rows[0]?.inserted_rows ?? 0)
		totalInserted += insertedRows
		if (insertedRows === 0) break
	}

	// Only rebuild the stats tables when there is actually new data to reflect.
	// refresh_bunching_stats refreshes route, segment, and hourly stats in one
	// temp-table scan, so a single call covers all three tables.
	if (totalInserted > 0) {
		await query('select refresh_bunching_stats(30)')
		console.log(`Enrichment and stats refresh complete (${totalInserted} rows inserted)`)
	} else {
		console.log('No new rows to enrich, skipping stats refresh')
	}
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runEnrich()
		.then(async () => {
			await closePool()
			process.exit(0)
		})
		.catch(async (error) => {
			console.error('Enrichment job failed', error)
			await closePool()
			process.exit(1)
		})
}
