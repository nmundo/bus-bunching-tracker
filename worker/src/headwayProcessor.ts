import { fileURLToPath } from 'node:url'
import { query } from './db'

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

export const runHeadways = async () => {
	const watermark = await getWatermark()

	await query(
		`with ordered as (
      select
        route_id,
        direction_id,
        stop_id,
        vid,
        arrival_time,
        lag(vid) over w as prev_vid,
        lag(arrival_time) over w as prev_time
      from stop_arrivals
      where arrival_time >= coalesce($1, '1970-01-01'::timestamptz) - interval '2 hours'
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
      s.id
    from ordered o
    left join segments s
      on s.route_id = o.route_id
      and s.direction_id = o.direction_id
      and s.to_stop_id = o.stop_id
    where o.prev_time is not null
    on conflict do nothing`,
		[watermark]
	)

	const maxResult = await query<{ max_time: Date | null }>(
		`select max(arrival_time) as max_time from stop_arrivals`
	)
	const maxTime = maxResult.rows[0]?.max_time ?? null
	if (maxTime) {
		await setWatermark(maxTime)
	}
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	runHeadways()
		.then(() => {
			console.log('Headways processed')
			process.exit(0)
		})
		.catch((error) => {
			console.error('Headways processing failed', error)
			process.exit(1)
		})
}
