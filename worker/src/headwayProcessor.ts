import { fileURLToPath } from 'node:url'
import { query, closePool } from './db'

const getWatermark = async () => {
	const result = await query<{ watermark: Date | null }>(
		`select watermark from job_state where id = 'headways_processor'`
	)
	return result.rows[0]?.watermark ?? null
}

const setWatermark = async (watermark: Date) => {
	await query(
		`insert into job_state (id, watermark)
     values ('headways_processor', $1)
     on conflict (id) do update set watermark = excluded.watermark`,
		[watermark]
	)
}

export const HEADWAYS_INSERT_SQL = `
    with deduped as (
      select route_id, direction_id, stop_id, vid, arrival_time
      from (
        select
          route_id,
          direction_id,
          stop_id,
          vid,
          arrival_time,
          row_number() over (
            partition by route_id, direction_id, stop_id, arrival_time
            order by vid
          ) as rn
        from stop_arrivals
        where arrival_time >= coalesce($1, '1970-01-01'::timestamptz) - interval '2 hours'
      ) sub
      where rn = 1
    ),
    ordered as (
      select
        route_id,
        direction_id,
        stop_id,
        vid,
        arrival_time,
        lag(vid) over w as prev_vid,
        lag(arrival_time) over w as prev_time
      from deduped
      window w as (partition by route_id, direction_id, stop_id order by arrival_time)
    )
    insert into headways (
      route_id,
      direction_id,
      stop_id,
      prev_vid,
      curr_vid,
      arrival_time,
      headway_min,
      segment_id
    )
    select
      o.route_id,
      o.direction_id,
      o.stop_id,
      o.prev_vid,
      o.vid,
      o.arrival_time,
      extract(epoch from (o.arrival_time - o.prev_time)) / 60,
      seg.id
    from ordered o
    left join lateral (
      select s.id
      from segments s
      where s.route_id = o.route_id
        and s.to_stop_id = o.stop_id
      order by
        case
          when s.direction_id = o.direction_id then 0
          when s.direction_id is null then 2
          else 1
        end,
        s.id
      limit 1
    ) seg on true
    where o.prev_time is not null
      and o.arrival_time > o.prev_time
    on conflict do nothing
  `

// Kept for backwards-compat with any existing tests that import this name.
export const _buildHeadwaysInsertSql = () => HEADWAYS_INSERT_SQL

export const runHeadways = async () => {
	const watermark = await getWatermark()

	// Capture the ceiling BEFORE processing so new rows that land during the
	// insert do not advance the watermark past unprocessed data.
	const maxResult = await query<{ max_time: Date | null }>(
		`select max(arrival_time) as max_time from stop_arrivals`
	)
	const maxTime = maxResult.rows[0]?.max_time ?? null

	await query(HEADWAYS_INSERT_SQL, [watermark])

	if (maxTime) {
		await setWatermark(maxTime)
	}
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runHeadways()
		.then(async () => {
			console.log('Headways processed')
			await closePool()
			process.exit(0)
		})
		.catch(async (error) => {
			console.error('Headways processing failed', error)
			await closePool()
			process.exit(1)
		})
}
