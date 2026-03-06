create or replace function enrich_headways_batch(p_limit integer default 10000)
returns integer
language plpgsql
as $$
declare
  v_rows integer;
begin
  with source_headways as (
    select h.*
    from headways h
    left join headways_enriched he
      on he.route_id = h.route_id
      and he.stop_id = h.stop_id
      and he.arrival_time = h.arrival_time
      and he.actual_headway_min = h.headway_min
    where he.id is null
    order by h.arrival_time, h.route_id, h.stop_id, h.id
    limit p_limit
  ),
  prepared as (
    select
      h.*,
      tz.local_ts::date as local_date,
      (date_trunc('hour', tz.local_ts)
        + floor(extract(minute from tz.local_ts) / 15) * interval '15 minutes'
      )::time as time_bin_start
    from source_headways h
    cross join lateral (
      select (h.arrival_time at time zone 'America/Chicago') as local_ts
    ) tz
  ),
  inserted as (
    insert into headways_enriched (
      route_id,
      direction_id,
      stop_id,
      segment_id,
      arrival_time,
      service_id,
      time_of_day_bucket,
      time_bin_start,
      actual_headway_min,
      scheduled_headway_min,
      hw_ratio,
      bunched,
      super_bunched,
      gapped
    )
    select
      h.route_id,
      h.direction_id,
      h.stop_id,
      h.segment_id,
      h.arrival_time,
      coalesce(sh.service_id, service_id_for_date(h.local_date)) as service_id,
      time_of_day_bucket(h.arrival_time) as time_of_day_bucket,
      h.time_bin_start,
      h.headway_min as actual_headway_min,
      sh.scheduled_headway_min,
      h.headway_min / nullif(sh.scheduled_headway_min, 0) as hw_ratio,
      (sh.scheduled_headway_min is not null and h.headway_min < 0.25 * sh.scheduled_headway_min) as bunched,
      (h.headway_min <= 1.0) as super_bunched,
      (sh.scheduled_headway_min is not null and h.headway_min > 1.75 * sh.scheduled_headway_min) as gapped
    from prepared h
    left join lateral (
      select
        candidate.service_id,
        candidate.scheduled_headway_min
      from (
        select
          0 as scope_rank,
          sh.service_id,
          sh.direction_id,
          sh.time_bin_start,
          sh.scheduled_headway_min
        from scheduled_headways sh
        where sh.route_id = h.route_id
          and sh.stop_id = h.stop_id

        union all

        select
          1 as scope_rank,
          sh.service_id,
          sh.direction_id,
          sh.time_bin_start,
          avg(sh.scheduled_headway_min) as scheduled_headway_min
        from scheduled_headways sh
        where sh.route_id = h.route_id
          and sh.scheduled_headway_min is not null
        group by sh.service_id, sh.direction_id, sh.time_bin_start
      ) candidate
      left join gtfs_calendar c on c.service_id = candidate.service_id
      left join gtfs_calendar_dates cd_add
        on cd_add.service_id = candidate.service_id
        and cd_add.date = h.local_date
        and cd_add.exception_type = 1
      left join gtfs_calendar_dates cd_remove
        on cd_remove.service_id = candidate.service_id
        and cd_remove.date = h.local_date
        and cd_remove.exception_type = 2
      order by
        candidate.scope_rank,
        case
          when cd_add.service_id is not null then 0
          when c.service_id is not null
            and h.local_date between c.start_date and c.end_date
            and (ARRAY[c.sunday, c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday])
                [extract(dow from h.local_date)::int + 1] = 1
            and cd_remove.service_id is null
          then 0
          else 1
        end,
        case
          when candidate.direction_id is null
            or h.direction_id is null
            or candidate.direction_id = h.direction_id
          then 0
          else 1
        end,
        abs(extract(epoch from (candidate.time_bin_start - h.time_bin_start)))
      limit 1
    ) sh on true
    returning 1
  )
  select count(*) into v_rows from inserted;

  return coalesce(v_rows, 0);
end;
$$;
