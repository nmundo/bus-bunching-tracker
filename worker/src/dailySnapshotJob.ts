import { fileURLToPath } from 'node:url'
import { query, closePool } from './db'

export const DAILY_SNAPSHOT_SQL =
	'select snapshot_daily_bunching_stats($1::date) as rows'

// Capture one local (Chicago) calendar day of enriched headways into the daily
// trend table.  Pass null (the default) to snapshot "yesterday", which the SQL
// function resolves so the captured day is complete.
export const runDailySnapshot = async (date: string | null = null) => {
	const result = await query<{ rows: number | string | null }>(DAILY_SNAPSHOT_SQL, [date])
	const rows = Number(result.rows[0]?.rows ?? 0)
	console.log(`Daily snapshot complete (${rows} route-rows for ${date ?? 'yesterday'})`)
	return rows
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	// Optional CLI arg: a YYYY-MM-DD date to backfill a specific day.
	const date = process.argv[2] ?? null
	runDailySnapshot(date)
		.then(async () => {
			await closePool()
			process.exit(0)
		})
		.catch(async (error) => {
			console.error('Daily snapshot failed', error)
			await closePool()
			process.exit(1)
		})
}
