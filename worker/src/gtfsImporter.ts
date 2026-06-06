import { createReadStream, createWriteStream } from 'node:fs'
import { mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pipeline } from 'node:stream/promises'
import { Readable } from 'node:stream'
import unzipper from 'unzipper'
import { parse } from 'csv-parse'
import { query, getPool, closePool } from './db'

const GTFS_URL = 'https://www.transitchicago.com/downloads/sch_data/google_transit.zip'

type TableConfig = {
	table: string
	columns: string[]
	conflict: string[]
	mapRow: (row: Record<string, string>) => (string | number | null)[]
}

const toInt = (value: string | undefined) => (value ? Number.parseInt(value, 10) : null)
const toFloat = (value: string | undefined) => (value ? Number.parseFloat(value) : null)
const toDate = (value: string | undefined) => {
	if (!value) return null
	if (value.includes('-')) return value
	return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
}

/**
 * Normalise a GTFS time string to a zero-padded HH:MM:SS value.
 *
 * GTFS allows times past 24:00 for overnight trips (e.g. "25:30:00" = 01:30 AM
 * the following service day). The gtfs_stop_times and gtfs_frequencies columns
 * are stored as `interval`, which accepts these values natively, so no wrapping
 * is needed.
 *
 * If the value is malformed we return it as-is and let Postgres surface a
 * clear error rather than silently swallowing bad data.
 */
const normalizeTime = (value: string | undefined): string | null => {
	if (!value) return null
	const trimmed = value.trim()
	const parts = trimmed.split(':')
	if (parts.length < 2 || parts.length > 3) return trimmed
	const [rawH, rawM, rawS = '00'] = parts
	const h = Number.parseInt(rawH, 10)
	const m = Number.parseInt(rawM, 10)
	const s = Number.parseInt(rawS, 10)
	if (Number.isNaN(h) || Number.isNaN(m) || Number.isNaN(s)) return trimmed
	return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`
}

const TABLES: Record<string, TableConfig> = {
	'routes.txt': {
		table: 'gtfs_routes',
		columns: ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type'],
		conflict: ['route_id'],
		mapRow: (row) => [
			row.route_id,
			row.agency_id,
			row.route_short_name,
			row.route_long_name,
			toInt(row.route_type)
		]
	},
	'stops.txt': {
		table: 'gtfs_stops',
		columns: ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
		conflict: ['stop_id'],
		mapRow: (row) => [row.stop_id, row.stop_name, toFloat(row.stop_lat), toFloat(row.stop_lon)]
	},
	'trips.txt': {
		table: 'gtfs_trips',
		columns: ['trip_id', 'route_id', 'service_id', 'shape_id', 'direction_id'],
		conflict: ['trip_id'],
		mapRow: (row) => [
			row.trip_id,
			row.route_id,
			row.service_id,
			row.shape_id,
			toInt(row.direction_id)
		]
	},
	'stop_times.txt': {
		table: 'gtfs_stop_times',
		columns: ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence'],
		conflict: ['trip_id', 'stop_sequence'],
		mapRow: (row) => [
			row.trip_id,
			normalizeTime(row.arrival_time),
			normalizeTime(row.departure_time),
			row.stop_id,
			toInt(row.stop_sequence)
		]
	},
	'shapes.txt': {
		table: 'gtfs_shapes',
		columns: ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'],
		conflict: ['shape_id', 'shape_pt_sequence'],
		mapRow: (row) => [
			row.shape_id,
			toFloat(row.shape_pt_lat),
			toFloat(row.shape_pt_lon),
			toInt(row.shape_pt_sequence)
		]
	},
	'calendar.txt': {
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
		conflict: ['service_id'],
		mapRow: (row) => [
			row.service_id,
			toInt(row.monday),
			toInt(row.tuesday),
			toInt(row.wednesday),
			toInt(row.thursday),
			toInt(row.friday),
			toInt(row.saturday),
			toInt(row.sunday),
			toDate(row.start_date),
			toDate(row.end_date)
		]
	},
	'calendar_dates.txt': {
		table: 'gtfs_calendar_dates',
		columns: ['service_id', 'date', 'exception_type'],
		conflict: ['service_id', 'date'],
		mapRow: (row) => [row.service_id, toDate(row.date), toInt(row.exception_type)]
	},
	'frequencies.txt': {
		table: 'gtfs_frequencies',
		columns: ['trip_id', 'start_time', 'end_time', 'headway_secs'],
		conflict: ['trip_id', 'start_time', 'end_time'],
		mapRow: (row) => [
			row.trip_id,
			normalizeTime(row.start_time),
			normalizeTime(row.end_time),
			toInt(row.headway_secs)
		]
	}
}

const batchUpsert = async (
	table: string,
	columns: string[],
	conflict: string[],
	rows: (string | number | null)[][]
) => {
	if (rows.length === 0) return

	const valuePlaceholders = rows
		.map((row, rowIndex) => {
			const offset = rowIndex * columns.length
			const placeholders = columns.map((_, colIndex) => `$${offset + colIndex + 1}`)
			return `(${placeholders.join(', ')})`
		})
		.join(', ')

	const updates = columns
		.filter((col) => !conflict.includes(col))
		.map((col) => `${col} = excluded.${col}`)
		.join(', ')

	const sql = `
    insert into ${table} (${columns.join(', ')})
    values ${valuePlaceholders}
    on conflict (${conflict.join(', ')}) do update set ${updates}
  `

	const flatValues = rows.flat()
	await query(sql, flatValues)
}

/**
 * Stream-parse a CSV file and upsert rows in batches of 1000.
 *
 * Uses `for await...of` on the csv-parse async iterator so backpressure and
 * stream completion are handled by the runtime. This avoids the race condition
 * in the previous event-listener approach where `pipeline()` could resolve
 * before the last `readable` handler's `await batchUpsert` finished.
 */
const importFile = async (stream: NodeJS.ReadableStream, config: TableConfig) => {
	const parser = stream.pipe(parse({ columns: true, trim: true, skip_empty_lines: true }))
	const batch: (string | number | null)[][] = []

	for await (const record of parser) {
		batch.push(config.mapRow(record as Record<string, string>))
		if (batch.length >= 1000) {
			await batchUpsert(config.table, config.columns, config.conflict, batch.splice(0))
		}
	}

	if (batch.length > 0) {
		await batchUpsert(config.table, config.columns, config.conflict, batch)
	}
}

const refreshStopGeometry = async () => {
	await query(`
    update gtfs_stops
    set geom = ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)
    where stop_lon is not null and stop_lat is not null
  `)
}

/**
 * Rebuild canonical stop sequences and route segments inside a single
 * transaction so that a mid-run failure never leaves the tables empty.
 * Previously these were plain sequential queries; a crash after the DELETEs
 * but before the INSERTs would leave segments empty and break FK constraints.
 */
const buildCanonicalSequences = async () => {
	const client = await getPool().connect()
	try {
		await client.query('begin')

		// Null out FK references to segments before deleting them.
		// headways.segment_id and headways_enriched.segment_id both reference
		// segments(id). We preserve the headway rows — segment_ids will be
		// backfilled by backfill_headways_segment_ids() after the new segments
		// are inserted.
		await client.query('update headways set segment_id = null where segment_id is not null')
		await client.query(
			'update headways_enriched set segment_id = null where segment_id is not null'
		)
		await client.query(
			'update segment_bunching_stats set segment_id = null where segment_id is not null'
		)
		await client.query('delete from route_stop_sequences')
		await client.query('delete from segments')

		await client.query(`
      with canonical as (
        select route_id, direction_id, trip_id, shape_id
        from (
          select t.route_id, t.direction_id, t.trip_id, t.shape_id,
            count(*) as stop_count,
            row_number() over (partition by t.route_id, t.direction_id order by count(*) desc) as rn
          from gtfs_trips t
          join gtfs_stop_times st on st.trip_id = t.trip_id
          group by t.route_id, t.direction_id, t.trip_id, t.shape_id
        ) ranked
        where rn = 1
      ),
      shape_lines as (
        select shape_id,
          ST_MakeLine(
            array_agg(ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326) order by shape_pt_sequence)
          ) as geom
        from gtfs_shapes
        group by shape_id
      ),
      stops as (
        select c.route_id, c.direction_id, c.trip_id, c.shape_id,
          st.stop_sequence, st.stop_id,
          s.geom as stop_geom,
          sl.geom as shape_geom,
          ST_LineLocatePoint(sl.geom, s.geom) as frac_along,
          ST_Length(sl.geom::geography) as shape_length_m
        from canonical c
        join gtfs_stop_times st on st.trip_id = c.trip_id
        join gtfs_stops s on s.stop_id = st.stop_id
        join shape_lines sl on sl.shape_id = c.shape_id
      )
      insert into route_stop_sequences (
        route_id,
        direction_id,
        stop_sequence,
        stop_id,
        shape_id,
        cumulative_distance_m
      )
      select
        route_id,
        direction_id,
        stop_sequence,
        stop_id,
        shape_id,
        (frac_along * shape_length_m)
      from stops
      order by route_id, direction_id, stop_sequence
    `)

		await client.query(`
      with shape_lines as (
        select shape_id,
          ST_MakeLine(
            array_agg(ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326) order by shape_pt_sequence)
          ) as geom,
          ST_Length(ST_MakeLine(
            array_agg(ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326) order by shape_pt_sequence)
          )::geography) as length_m
        from gtfs_shapes
        group by shape_id
      ),
      seqs as (
        select
          rss.*,
          lag(rss.stop_id) over w as prev_stop_id,
          lag(rss.cumulative_distance_m) over w as prev_dist
        from route_stop_sequences rss
        window w as (partition by route_id, direction_id order by stop_sequence)
      )
      insert into segments (
        route_id,
        direction_id,
        from_stop_id,
        to_stop_id,
        geom
      )
      select
        s.route_id,
        s.direction_id,
        s.prev_stop_id,
        s.stop_id,
        ST_LineSubstring(
          sl.geom,
          greatest(0, least(1, least(s.prev_dist, s.cumulative_distance_m) / nullif(sl.length_m, 0))),
          greatest(0, least(1, greatest(s.prev_dist, s.cumulative_distance_m) / nullif(sl.length_m, 0)))
        )
      from seqs s
      join shape_lines sl on sl.shape_id = s.shape_id
      where s.prev_stop_id is not null
    `)

		await client.query(`
      update route_stop_sequences rss
      set segment_id = seg.id
      from segments seg
      where rss.route_id = seg.route_id
        and rss.direction_id = seg.direction_id
        and rss.stop_id = seg.to_stop_id
    `)

		await client.query('commit')
	} catch (err) {
		await client.query('rollback')
		throw err
	} finally {
		client.release()
	}
}

const computeScheduledHeadways = async () => {
	await query('delete from scheduled_headways')
	await query(`
    with bins as (
      select (time '00:00' + (interval '15 minutes' * gs))::time as time_bin_start
      from generate_series(0, 95) gs
    ),
    departures as (
      select t.route_id, t.direction_id, st.stop_id, t.service_id,
        st.departure_time
      from gtfs_trips t
      join gtfs_stop_times st on st.trip_id = t.trip_id
    ),
    binned as (
      select d.*, b.time_bin_start,
        (b.time_bin_start + interval '15 minutes')::time as time_bin_end
      from departures d
      join bins b
        on d.departure_time >= b.time_bin_start::interval
       and d.departure_time < (b.time_bin_start + interval '15 minutes')::interval
    ),
    ordered as (
      select *,
        extract(epoch from (departure_time - lag(departure_time) over (
          partition by route_id, direction_id, stop_id, service_id, time_bin_start
          order by departure_time
        ))) / 60 as headway_min
      from binned
    )
    insert into scheduled_headways (
      route_id,
      direction_id,
      stop_id,
      service_id,
      time_bin_start,
      time_bin_end,
      scheduled_headway_min
    )
    select
      route_id,
      direction_id,
      stop_id,
      service_id,
      time_bin_start,
      time_bin_end,
      avg(headway_min)
    from ordered
    where headway_min is not null
    group by route_id, direction_id, stop_id, service_id, time_bin_start, time_bin_end
  `)
}

const synthesizeFrequenciesIfEmpty = async () => {
	const countResult = await query<{ count: string }>('select count(*) from gtfs_frequencies')
	const existingCount = Number(countResult.rows[0]?.count ?? 0)
	if (existingCount > 0) {
		return
	}

	const result = await query<{ inserted_count: string }>(`
    with canonical_trips as (
      select route_id, direction_id, service_id, trip_id
      from (
        select
          t.route_id,
          t.direction_id,
          t.service_id,
          t.trip_id,
          count(st.stop_id) as stop_count,
          row_number() over (
            partition by t.route_id, t.direction_id, t.service_id
            order by count(st.stop_id) desc
          ) as rn
        from gtfs_trips t
        join gtfs_stop_times st on st.trip_id = t.trip_id
        group by t.route_id, t.direction_id, t.service_id, t.trip_id
      ) ranked
      where rn = 1
    ),
    first_stop as (
      select distinct on (st.trip_id)
        st.trip_id,
        st.stop_id
      from gtfs_stop_times st
      join canonical_trips ct on ct.trip_id = st.trip_id
      order by st.trip_id, st.stop_sequence
    ),
    inserted as (
      insert into gtfs_frequencies (trip_id, start_time, end_time, headway_secs)
      select
        ct.trip_id,
        sh.time_bin_start,
        sh.time_bin_end,
        round(sh.scheduled_headway_min * 60)::int
      from scheduled_headways sh
      join canonical_trips ct
        on ct.route_id = sh.route_id
        and ct.direction_id = sh.direction_id
        and ct.service_id = sh.service_id
      join first_stop fs
        on fs.trip_id = ct.trip_id
        and fs.stop_id = sh.stop_id
      where sh.scheduled_headway_min is not null
      on conflict (trip_id, start_time, end_time) do update
        set headway_secs = excluded.headway_secs
      returning 1
    )
    select count(*) as inserted_count from inserted
  `)

	const insertedCount = Number(result.rows[0]?.inserted_count ?? 0)
	console.log(`Synthesized ${insertedCount} gtfs_frequencies rows`)
}

const runImport = async () => {
	const res = await fetch(GTFS_URL)
	if (!res.ok || !res.body) {
		throw new Error(`Failed to download GTFS zip: ${res.status}`)
	}

	const stream = Readable.fromWeb(res.body as never)
	const zip = stream.pipe(unzipper.Parse({ forceStream: true }))
	const workspace = join(tmpdir(), `gtfs-${Date.now()}`)
	await mkdir(workspace, { recursive: true })

	try {
		const extractedFiles: Record<string, string> = {}

		for await (const entry of zip) {
			if (!TABLES[entry.path]) {
				entry.autodrain()
				continue
			}
			const filePath = join(workspace, entry.path)
			const writeStream = createWriteStream(filePath)
			await pipeline(entry, writeStream)
			extractedFiles[entry.path] = filePath
		}

		const importOrder = [
			'routes.txt',
			'stops.txt',
			'trips.txt',
			'stop_times.txt',
			'shapes.txt',
			'calendar.txt',
			'calendar_dates.txt',
			'frequencies.txt'
		]

		for (const fileName of importOrder) {
			const filePath = extractedFiles[fileName]
			if (!filePath) continue
			const config = TABLES[fileName]
			await importFile(createReadStream(filePath), config)
		}
	} finally {
		await rm(workspace, { recursive: true, force: true })
	}

	await refreshStopGeometry()
	await buildCanonicalSequences()
	await query('SELECT refresh_route_direction_labels()')
	await computeScheduledHeadways()
	await synthesizeFrequenciesIfEmpty()
}

runImport()
	.then(async () => {
		console.log('GTFS import complete')
		await closePool()
		process.exit(0)
	})
	.catch(async (error) => {
		console.error('GTFS import failed', error)
		await closePool()
		process.exit(1)
	})
