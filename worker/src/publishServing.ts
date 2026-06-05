import pg from 'pg'
import 'dotenv/config'
import { fileURLToPath } from 'node:url'
import { getPool } from './db'
import { runEnrich } from './enrichJob'

const { Pool } = pg

const DEFAULT_WINDOW_DAYS = 30
const DEFAULT_BATCH_SIZE = 500
const SERVING_ADVISORY_LOCK_KEY = 913_447_221

type InsertableRow = Record<string, unknown>

// ── helpers ──────────────────────────────────────────────────────────────────

const chunk = <T>(items: T[], size: number): T[][] => {
	const chunks: T[][] = []
	for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size))
	return chunks
}

/**
 * Build a parameterised INSERT … ON CONFLICT (conflictCols) DO UPDATE SET …
 * statement.  Pass an empty conflictCols array to get a plain INSERT (no
 * conflict handling).
 */
const buildUpsertSql = (
	table: string,
	columns: string[],
	conflictColumns: string[],
	rowCount: number
): string => {
	const colsSql = columns.map((c) => `"${c}"`).join(', ')
	const valuesSql = Array.from({ length: rowCount }, (_, rowIndex) => {
		const placeholders = columns.map(
			(_, colIndex) => `$${rowIndex * columns.length + colIndex + 1}`
		)
		return `(${placeholders.join(', ')})`
	}).join(', ')

	if (conflictColumns.length === 0) {
		return `INSERT INTO ${table} (${colsSql}) VALUES ${valuesSql}`
	}

	const updateCols = columns.filter((c) => !conflictColumns.includes(c))
	const updateSql = updateCols.map((c) => `"${c}" = excluded."${c}"`).join(', ')
	const conflictSql = conflictColumns.map((c) => `"${c}"`).join(', ')
	return `INSERT INTO ${table} (${colsSql}) VALUES ${valuesSql}
    ON CONFLICT (${conflictSql}) DO UPDATE SET ${updateSql}`
}

/**
 * Batch-upsert rows into a serving-DB table.
 * conflictColumns: primary-key / unique columns to match on for DO UPDATE.
 */
const upsertBatched = async (
	client: pg.PoolClient,
	{
		table,
		columns,
		conflictColumns,
		rows,
		batchSize = DEFAULT_BATCH_SIZE
	}: {
		table: string
		columns: string[]
		conflictColumns: string[]
		rows: InsertableRow[]
		batchSize?: number
	}
) => {
	if (rows.length === 0) return
	for (const batch of chunk(rows, batchSize)) {
		const sql = buildUpsertSql(table, columns, conflictColumns, batch.length)
		const params: unknown[] = []
		for (const row of batch) {
			for (const col of columns) params.push(row[col])
		}
		await client.query(sql, params)
	}
}

/**
 * After upserting a reference table, remove rows whose key no longer exists in
 * the source.  Uses a Postgres array parameter to avoid N individual deletes.
 */
const deleteObsoleteRows = async (
	client: pg.PoolClient,
	table: string,
	keyColumn: string,
	pgType: string,
	keepValues: unknown[]
) => {
	if (keepValues.length === 0) {
		// No new data at all — clear the table entirely.
		await client.query(`DELETE FROM ${table}`)
	} else {
		await client.query(
			`DELETE FROM ${table} WHERE "${keyColumn}" != ALL($1::${pgType}[])`,
			[keepValues]
		)
	}
}

// ── main ─────────────────────────────────────────────────────────────────────

export const runPublishServing = async () => {
	const warehouseUrl = process.env.DATABASE_URL
	const servingUrl = process.env.SERVING_DATABASE_URL
	if (!warehouseUrl) throw new Error('DATABASE_URL is not set (warehouse)')
	if (!servingUrl) throw new Error('SERVING_DATABASE_URL is not set (serving)')

	const windowDays = Number(process.env.SERVING_WINDOW_DAYS ?? DEFAULT_WINDOW_DAYS)
	if (!Number.isFinite(windowDays) || windowDays <= 0) {
		throw new Error(`Invalid SERVING_WINDOW_DAYS: ${process.env.SERVING_WINDOW_DAYS}`)
	}

	await runEnrich({ maxBatches: 120 })

	// Re-use the shared warehouse pool (already initialised by runEnrich above).
	const servingPool = new Pool({ connectionString: servingUrl })
	const warehouseClient = await getPool().connect()
	const servingClient = await servingPool.connect()

	try {
		// Advisory lock prevents concurrent publish runs from clobbering each other.
		const lock = await servingClient.query<{ locked: boolean }>(
			'SELECT pg_try_advisory_lock($1)::boolean AS locked',
			[SERVING_ADVISORY_LOCK_KEY]
		)
		if (!lock.rows[0]?.locked) {
			console.warn('Serving publish skipped: advisory lock not acquired')
			return
		}

		try {
			// Fetch all source data from the warehouse in parallel.
			const [routes, stops, calendar, segments, routeStats, segmentStats, hourlyStats, maxArrival] =
				await Promise.all([
					warehouseClient.query<{ route_id: string; route_short_name: string; route_long_name: string }>(
						`SELECT route_id, route_short_name, route_long_name
             FROM gtfs_routes
             ORDER BY route_short_name`
					),
					warehouseClient.query<{ stop_id: string; stop_name: string }>(
						`SELECT stop_id, stop_name FROM gtfs_stops`
					),
					warehouseClient.query(
						`SELECT service_id, monday, tuesday, wednesday, thursday, friday,
                    saturday, sunday, start_date, end_date
             FROM gtfs_calendar`
					),
					warehouseClient.query(
						`SELECT id, route_id, direction_id, from_stop_id, to_stop_id,
                    coalesce(geometry, st_asgeojson(geom)::jsonb) AS geometry
             FROM segments`
					),
					warehouseClient.query(
						`SELECT route_id,
                    coalesce(direction_id, -1)        AS direction_id,
                    coalesce(service_id, 'unknown')   AS service_id,
                    time_of_day_bucket,
                    total_headways,
                    bunched_headways,
                    super_bunched_headways,
                    bunching_rate,
                    avg_hw_ratio,
                    median_scheduled_headway,
                    median_actual_headway,
                    coalesce(gapped_headways, 0)      AS gapped_headways
             FROM route_bunching_stats`
					),
					warehouseClient.query(
						`SELECT segment_id,
                    route_id,
                    coalesce(service_id, 'unknown')   AS service_id,
                    time_of_day_bucket,
                    sum(total_headways)::int           AS total_headways,
                    sum(bunched_headways)::int         AS bunched_headways,
                    CASE WHEN sum(total_headways) > 0
                         THEN sum(bunched_headways)::float / sum(total_headways)
                         ELSE null END                AS bunching_rate,
                    -1::int                           AS direction_id
             FROM segment_bunching_stats
             WHERE segment_id IS NOT NULL
             GROUP BY segment_id, route_id, coalesce(service_id, 'unknown'), time_of_day_bucket`
					),
					warehouseClient.query(
						`SELECT route_id,
                    coalesce(service_id, 'unknown')   AS service_id,
                    hour_of_day,
                    total_headways,
                    bunched_headways,
                    computed_at,
                    window_days
             FROM route_hourly_bunching_stats`
					),
					warehouseClient.query<{ max_observed_arrival_time: string | null }>(
						`SELECT max(arrival_time) AS max_observed_arrival_time FROM headways_enriched`
					)
				])

			await servingClient.query('BEGIN')
			try {
				// ── Reference tables: upsert + prune obsolete rows ────────────────
				// These change only at GTFS import time, so most upserts are no-ops.
				// We avoid wiping and rewriting 14 MB of segment geometry every day.

				await upsertBatched(servingClient, {
					table: 'gtfs_routes',
					columns: ['route_id', 'route_short_name', 'route_long_name'],
					conflictColumns: ['route_id'],
					rows: routes.rows
				})
				await deleteObsoleteRows(
					servingClient, 'gtfs_routes', 'route_id', 'text',
					routes.rows.map((r) => r.route_id)
				)

				await upsertBatched(servingClient, {
					table: 'gtfs_stops',
					columns: ['stop_id', 'stop_name'],
					conflictColumns: ['stop_id'],
					rows: stops.rows
				})
				await deleteObsoleteRows(
					servingClient, 'gtfs_stops', 'stop_id', 'text',
					stops.rows.map((r) => r.stop_id)
				)

				await upsertBatched(servingClient, {
					table: 'gtfs_calendar',
					columns: ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday',
						'friday', 'saturday', 'sunday', 'start_date', 'end_date'],
					conflictColumns: ['service_id'],
					rows: calendar.rows
				})
				await deleteObsoleteRows(
					servingClient, 'gtfs_calendar', 'service_id', 'text',
					calendar.rows.map((r) => r.service_id)
				)

				await upsertBatched(servingClient, {
					table: 'segments',
					columns: ['id', 'route_id', 'direction_id', 'from_stop_id', 'to_stop_id', 'geometry'],
					conflictColumns: ['id'],
					rows: segments.rows,
					batchSize: 200
				})
				await deleteObsoleteRows(
					servingClient, 'segments', 'id', 'uuid',
					segments.rows.map((r) => r.id)
				)

				// ── Stats tables: replace in-place ────────────────────────────────
				// Stats are fully recomputed on every publish cycle; a plain
				// DELETE + INSERT inside the open transaction is the simplest correct
				// approach (readers see old data until COMMIT, never empty data).

				await servingClient.query('DELETE FROM route_bunching_stats')
				await upsertBatched(servingClient, {
					table: 'route_bunching_stats',
					columns: [
						'route_id', 'direction_id', 'service_id', 'time_of_day_bucket',
						'total_headways', 'bunched_headways', 'super_bunched_headways',
						'bunching_rate', 'avg_hw_ratio', 'median_scheduled_headway',
						'median_actual_headway', 'gapped_headways'
					],
					conflictColumns: ['route_id', 'direction_id', 'service_id', 'time_of_day_bucket'],
					rows: routeStats.rows
				})

				await servingClient.query('DELETE FROM segment_bunching_stats')
				await upsertBatched(servingClient, {
					table: 'segment_bunching_stats',
					columns: [
						'segment_id', 'route_id', 'direction_id', 'service_id',
						'time_of_day_bucket', 'total_headways', 'bunched_headways', 'bunching_rate'
					],
					conflictColumns: ['segment_id', 'service_id', 'time_of_day_bucket'],
					rows: segmentStats.rows
				})

				await servingClient.query('DELETE FROM route_hourly_bunching_stats')
				await upsertBatched(servingClient, {
					table: 'route_hourly_bunching_stats',
					columns: [
						'route_id', 'service_id', 'hour_of_day',
						'total_headways', 'bunched_headways', 'computed_at', 'window_days'
					],
					conflictColumns: ['route_id', 'service_id', 'hour_of_day'],
					rows: hourlyStats.rows
				})

				// ── Metadata ──────────────────────────────────────────────────────
				const maxObservedArrivalTime = maxArrival.rows[0]?.max_observed_arrival_time ?? null
				await servingClient.query(
					`INSERT INTO publish_meta (id, last_published_at, window_days, max_observed_arrival_time)
           VALUES (1, now(), $1::int, $2::timestamptz)
           ON CONFLICT (id) DO UPDATE SET
             last_published_at        = excluded.last_published_at,
             window_days              = excluded.window_days,
             max_observed_arrival_time = excluded.max_observed_arrival_time`,
					[windowDays, maxObservedArrivalTime]
				)

				await servingClient.query('COMMIT')
				console.log(
					`Publish complete — ${routes.rows.length} routes, ${segments.rows.length} segments, ` +
					`${routeStats.rows.length} route stats, ${segmentStats.rows.length} segment stats`
				)
			} catch (error) {
				await servingClient.query('ROLLBACK')
				throw error
			}
		} finally {
			await servingClient.query('SELECT pg_advisory_unlock($1)', [SERVING_ADVISORY_LOCK_KEY])
		}
	} finally {
		servingClient.release()
		warehouseClient.release()
		await servingPool.end()
	}
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runPublishServing().catch((error) => {
		console.error('Serving publish failed', error)
		process.exit(1)
	})
}
