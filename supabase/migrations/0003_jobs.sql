create or replace function service_id_for_date(p_date date)
returns text
language sql
as $$
  select service_id
  from (
    select 0 as priority, cd.service_id
    from gtfs_calendar_dates cd
    where cd.date = p_date
      and cd.exception_type = 1

    union all

    select 1 as priority, c.service_id
    from gtfs_calendar c
    where p_date between c.start_date and c.end_date
      and (ARRAY[c.sunday, c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday])
          [extract(dow from p_date)::int + 1] = 1
      and not exists (
        select 1
        from gtfs_calendar_dates cd
        where cd.service_id = c.service_id
          and cd.date = p_date
          and cd.exception_type = 2
      )
  ) s
  order by priority, service_id
  limit 1;
$$;

create or replace function time_of_day_bucket(p_ts timestamptz)
returns text
language sql
as $$
  select
    case
      when (p_ts at time zone 'America/Chicago')::time >= time '07:00'
        and (p_ts at time zone 'America/Chicago')::time < time '10:00' then 'AM_peak'
      when (p_ts at time zone 'America/Chicago')::time >= time '10:00'
        and (p_ts at time zone 'America/Chicago')::time < time '15:00' then 'Midday'
      when (p_ts at time zone 'America/Chicago')::time >= time '15:00'
        and (p_ts at time zone 'America/Chicago')::time < time '19:00' then 'PM_peak'
      when (p_ts at time zone 'America/Chicago')::time >= time '19:00'
        and (p_ts at time zone 'America/Chicago')::time < time '23:00' then 'Evening'
      else 'Night'
    end;
$$;

create or replace function enrich_headways()
returns void
language plpgsql
as $$
begin
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
  from (
    select
      h.*,
      tz.local_ts::date as local_date,
      (date_trunc('hour', tz.local_ts)
        + floor(extract(minute from tz.local_ts) / 15) * interval '15 minutes'
      )::time as time_bin_start
    from headways h
    cross join lateral (
      select (h.arrival_time at time zone 'America/Chicago') as local_ts
    ) tz
  ) h
  left join headways_enriched he
    on he.route_id = h.route_id
    and he.stop_id = h.stop_id
    and he.arrival_time = h.arrival_time
    and he.actual_headway_min = h.headway_min
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
  where he.id is null;
end;
$$;

create or replace function refresh_route_bunching_stats(p_days integer default 30)
returns void
language plpgsql
as $$
begin
  delete from route_bunching_stats;

  insert into route_bunching_stats (
    route_id,
    direction_id,
    service_id,
    time_of_day_bucket,
    total_headways,
    bunched_headways,
    super_bunched_headways,
    bunching_rate,
    avg_hw_ratio,
    median_actual_headway
  )
  select
    route_id,
    direction_id,
    service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time)) as time_of_day_bucket,
    count(*) as total_headways,
    count(*) filter (where bunched) as bunched_headways,
    count(*) filter (where super_bunched) as super_bunched_headways,
    avg((bunched)::int)::float as bunching_rate,
    avg(hw_ratio) as avg_hw_ratio,
    percentile_cont(0.5) within group (order by actual_headway_min) as median_actual_headway
  from headways_enriched he
  where arrival_time >= now() - (p_days * interval '1 day')
  group by
    route_id,
    direction_id,
    service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time));
end;
$$;

create or replace function refresh_segment_bunching_stats(p_days integer default 30)
returns void
language plpgsql
as $$
begin
  delete from segment_bunching_stats;

  insert into segment_bunching_stats (
    segment_id,
    route_id,
    direction_id,
    service_id,
    time_of_day_bucket,
    total_headways,
    bunched_headways,
    bunching_rate
  )
  select
    segment_id,
    route_id,
    direction_id,
    service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time)) as time_of_day_bucket,
    count(*) as total_headways,
    count(*) filter (where bunched) as bunched_headways,
    avg((bunched)::int)::float as bunching_rate
  from headways_enriched he
  where arrival_time >= now() - (p_days * interval '1 day')
  group by
    segment_id,
    route_id,
    direction_id,
    service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time));
end;
$$;
