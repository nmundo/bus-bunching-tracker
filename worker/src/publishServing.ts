import pg from 'pg'
import 'dotenv/config'
import { fileURLToPath } from 'node:url'
import { runEnrich } from './enrichJob'

const { Pool } = pg

const DEFAULT_WINDOW_DAYS = 30
const DEFAULT_BATCH_SIZE = 500
const SERVING_ADVISORY_LOCK_KEY = 913_447_221

type InsertableRow = Record<string, unknown>

const chunk = <T>(items: T[], size: number) => {
	const chunks: T[][] = []
	for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size))
	return chunks
}

const buildInsertSql = (table: string, columns: string[], rowCount: number) => {
	const colsSql = columns.map((c) => `"${c}"`).join(', ')
	const valuesSql = Array.from({ length: rowCount }, (_, rowIndex) => {
		const placeholders = columns.map((_, colIndex) => `$${rowIndex * columns.length + colIndex + 1}`)
		return `(${placeholders.join(', ')})`
	}).join(', ')
	return `insert into ${table} (${colsSql}) values ${valuesSql}`
}

const insertBatched = async (
	client: pg.PoolClient,
	{
		table,
		columns,
		rows,
		batchSize = DEFAULT_BATCH_SIZE
	}: { table: string; columns: string[]; rows: InsertableRow[]; batchSize?: number }
) => {
	if (rows.length === 0) return

	for (const batch of chunk(rows, batchSize)) {
		const sql = buildInsertSql(table, columns, batch.length)
		const params: unknown[] = []
		for (const row of batch) {
			for (const col of columns) params.push(row[col])
		}
		await client.query(sql, params)
	}
}

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

	const warehousePool = new Pool({ connectionString: warehouseUrl })
	const servingPool = new Pool({ connectionString: servingUrl })

	const warehouseClient = await warehousePool.connect()
	const servingClient = await servingPool.connect()

	try {
		const lock = await servingClient.query<{ locked: boolean }>(
			'select pg_try_advisory_lock($1)::boolean as locked',
			[SERVING_ADVISORY_LOCK_KEY]
		)
		if (!lock.rows[0]?.locked) {
			console.warn('Serving publish skipped: advisory lock not acquired')
			return
		}

		const [
			routes,
			stops,
			calendar,
			segments,
			routeStats,
			segmentStats,
			hourlyStats,
			maxArrival
		] = await Promise.all([
			warehouseClient.query(
				`select route_id, route_short_name, route_long_name from gtfs_routes order by route_short_name`
			),
			warehouseClient.query(`select stop_id, stop_name from gtfs_stops`),
			warehouseClient.query(
				`select service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date from gtfs_calendar`
			),
			warehouseClient.query(
				`
        select
          id,
          route_id,
          direction_id,
          from_stop_id,
          to_stop_id,
          coalesce(geometry, st_asgeojson(geom)::jsonb) as geometry
        from segments
      `
			),
			warehouseClient.query(
				`
        select
          route_id,
          coalesce(direction_id, -1) as direction_id,
          coalesce(service_id, 'unknown') as service_id,
          time_of_day_bucket,
          total_headways,
          bunched_headways,
          super_bunched_headways,
          bunching_rate,
          avg_hw_ratio,
          median_scheduled_headway,
          median_actual_headway
        from route_bunching_stats
      `
			),
			warehouseClient.query(
				`
        select
          segment_id,
          route_id,
          coalesce(service_id, 'unknown') as service_id,
          time_of_day_bucket,
          sum(total_headways)::int as total_headways,
          sum(bunched_headways)::int as bunched_headways,
          case
            when sum(total_headways) > 0
            then sum(bunched_headways)::float / sum(total_headways)
            else null
          end as bunching_rate,
          -1::int as direction_id
        from segment_bunching_stats
        group by segment_id, route_id, coalesce(service_id, 'unknown'), time_of_day_bucket
      `
			),
			warehouseClient.query(
				`
        select
          route_id,
          service_id,
          hour_of_day,
          total_headways,
          bunched_headways,
          computed_at,
          window_days
        from route_hourly_bunching_stats
      `
			),
			warehouseClient.query<{ max_observed_arrival_time: string | null }>(
				`select max(arrival_time) as max_observed_arrival_time from headways_enriched`
			)
		])

		await servingClient.query('begin')
		try {
			await servingClient.query(`
        truncate table
          segment_bunching_stats,
          route_bunching_stats,
          route_hourly_bunching_stats,
          segments,
          gtfs_stops,
          gtfs_calendar,
          gtfs_routes
      `)

			await insertBatched(servingClient, {
				table: 'gtfs_routes',
				columns: ['route_id', 'route_short_name', 'route_long_name'],
				rows: routes.rows
			})
			await insertBatched(servingClient, {
				table: 'gtfs_stops',
				columns: ['stop_id', 'stop_name'],
				rows: stops.rows
			})
			await insertBatched(servingClient, {
				table: 'gtfs_calendar',
				columns: [
					'service_id',
					'monday',
					'tuesday',
					'wednesday',
					'thursday',
					'friday',
					'saturday',
					'sunday',
					'start_date',
					'end_date'
				],
				rows: calendar.rows
			})
			await insertBatched(servingClient, {
				table: 'segments',
				columns: ['id', 'route_id', 'direction_id', 'from_stop_id', 'to_stop_id', 'geometry'],
				rows: segments.rows,
				batchSize: 200
			})
			await insertBatched(servingClient, {
				table: 'route_bunching_stats',
				columns: [
					'route_id',
					'direction_id',
					'service_id',
					'time_of_day_bucket',
					'total_headways',
					'bunched_headways',
					'super_bunched_headways',
					'bunching_rate',
					'avg_hw_ratio',
					'median_scheduled_headway',
					'median_actual_headway'
				],
				rows: routeStats.rows
			})
			await insertBatched(servingClient, {
				table: 'segment_bunching_stats',
				columns: [
					'segment_id',
					'route_id',
					'direction_id',
					'service_id',
					'time_of_day_bucket',
					'total_headways',
					'bunched_headways',
					'bunching_rate'
				],
				rows: segmentStats.rows
			})
			await insertBatched(servingClient, {
				table: 'route_hourly_bunching_stats',
				columns: [
					'route_id',
					'service_id',
					'hour_of_day',
					'total_headways',
					'bunched_headways',
					'computed_at',
					'window_days'
				],
				rows: hourlyStats.rows
			})

			const maxObservedArrivalTime = maxArrival.rows[0]?.max_observed_arrival_time ?? null
			await servingClient.query(
				`
        insert into publish_meta (id, last_published_at, window_days, max_observed_arrival_time)
        values (1, now(), $1::int, $2::timestamptz)
        on conflict (id)
        do update set
          last_published_at = excluded.last_published_at,
          window_days = excluded.window_days,
          max_observed_arrival_time = excluded.max_observed_arrival_time
      `,
				[windowDays, maxObservedArrivalTime]
			)

			await servingClient.query('commit')
		} catch (error) {
			await servingClient.query('rollback')
			throw error
		}
	} finally {
		servingClient.release()
		warehouseClient.release()
		await servingPool.end()
		await warehousePool.end()
	}
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runPublishServing().catch((error) => {
		console.error('Serving publish failed', error)
		process.exit(1)
	})
}
