create or replace function service_id_for_date(p_date date)
returns text
language sql
as $$
  select coalesce(
    (
      select service_id
      from gtfs_calendar_dates
      where date = p_date
        and exception_type = 1
      limit 1
    ),
    (
      select c.service_id
      from gtfs_calendar c
      where p_date between c.start_date and c.end_date
        and (
          case extract(dow from p_date)
            when 0 then c.sunday
            when 1 then c.monday
            when 2 then c.tuesday
            when 3 then c.wednesday
            when 4 then c.thursday
            when 5 then c.friday
            when 6 then c.saturday
          end
        ) = 1
        and not exists (
          select 1
          from gtfs_calendar_dates cd
          where cd.service_id = c.service_id
            and cd.date = p_date
            and cd.exception_type = 2
        )
      order by c.service_id
      limit 1
    )
  );
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
    service_id_for_date((h.arrival_time at time zone 'America/Chicago')::date) as service_id,
    (date_trunc('hour', h.arrival_time at time zone 'America/Chicago')
      + floor(extract(minute from h.arrival_time at time zone 'America/Chicago') / 15) * interval '15 minutes'
    )::time as time_bin_start,
    h.headway_min as actual_headway_min,
    sh.scheduled_headway_min,
    case
      when sh.scheduled_headway_min is not null and sh.scheduled_headway_min > 0
      then h.headway_min / sh.scheduled_headway_min
      else null
    end as hw_ratio,
    case
      when sh.scheduled_headway_min is not null and h.headway_min < 0.25 * sh.scheduled_headway_min then true
      else false
    end as bunched,
    case when h.headway_min <= 1.0 then true else false end as super_bunched,
    case
      when sh.scheduled_headway_min is not null and h.headway_min > 1.75 * sh.scheduled_headway_min then true
      else false
    end as gapped
  from headways h
  left join headways_enriched he
    on he.route_id = h.route_id
    and he.stop_id = h.stop_id
    and he.arrival_time = h.arrival_time
    and he.actual_headway_min = h.headway_min
  left join lateral (
    select sh.scheduled_headway_min
    from scheduled_headways sh
    where sh.route_id = h.route_id
      and sh.direction_id = h.direction_id
      and sh.stop_id = h.stop_id
      and sh.service_id = service_id_for_date((h.arrival_time at time zone 'America/Chicago')::date)
    order by abs(extract(epoch from (sh.time_bin_start - (
      date_trunc('hour', h.arrival_time at time zone 'America/Chicago')
      + floor(extract(minute from h.arrival_time at time zone 'America/Chicago') / 15) * interval '15 minutes'
    )::time)))
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
    case
      when (arrival_time at time zone 'America/Chicago')::time >= time '07:00'
        and (arrival_time at time zone 'America/Chicago')::time < time '10:00' then 'AM_peak'
      when (arrival_time at time zone 'America/Chicago')::time >= time '10:00'
        and (arrival_time at time zone 'America/Chicago')::time < time '15:00' then 'Midday'
      when (arrival_time at time zone 'America/Chicago')::time >= time '15:00'
        and (arrival_time at time zone 'America/Chicago')::time < time '19:00' then 'PM_peak'
      when (arrival_time at time zone 'America/Chicago')::time >= time '19:00'
        and (arrival_time at time zone 'America/Chicago')::time < time '23:00' then 'Evening'
      else 'Night'
    end as time_of_day_bucket,
    count(*) as total_headways,
    sum(case when bunched then 1 else 0 end) as bunched_headways,
    sum(case when super_bunched then 1 else 0 end) as super_bunched_headways,
    case when count(*) > 0 then sum(case when bunched then 1 else 0 end)::float / count(*) else null end as bunching_rate,
    avg(hw_ratio) as avg_hw_ratio,
    percentile_cont(0.5) within group (order by actual_headway_min) as median_actual_headway
  from headways_enriched
  where arrival_time >= now() - (p_days || ' days')::interval
  group by route_id, direction_id, service_id, time_of_day_bucket;
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
    case
      when (arrival_time at time zone 'America/Chicago')::time >= time '07:00'
        and (arrival_time at time zone 'America/Chicago')::time < time '10:00' then 'AM_peak'
      when (arrival_time at time zone 'America/Chicago')::time >= time '10:00'
        and (arrival_time at time zone 'America/Chicago')::time < time '15:00' then 'Midday'
      when (arrival_time at time zone 'America/Chicago')::time >= time '15:00'
        and (arrival_time at time zone 'America/Chicago')::time < time '19:00' then 'PM_peak'
      when (arrival_time at time zone 'America/Chicago')::time >= time '19:00'
        and (arrival_time at time zone 'America/Chicago')::time < time '23:00' then 'Evening'
      else 'Night'
    end as time_of_day_bucket,
    count(*) as total_headways,
    sum(case when bunched then 1 else 0 end) as bunched_headways,
    case when count(*) > 0 then sum(case when bunched then 1 else 0 end)::float / count(*) else null end as bunching_rate
  from headways_enriched
  where arrival_time >= now() - (p_days || ' days')::interval
  group by segment_id, route_id, direction_id, service_id, time_of_day_bucket;
end;
$$;
