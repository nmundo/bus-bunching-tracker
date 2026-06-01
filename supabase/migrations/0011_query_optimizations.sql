-- Query-path indexes for incremental worker jobs and serving/API lookups.
create index if not exists bus_positions_tmstmp_id_idx
  on bus_positions (tmstmp, id)
  where tmstmp is not null;

create index if not exists stop_arrivals_arrival_time_idx
  on stop_arrivals (arrival_time, route_id, direction_id, stop_id, vid);

create index if not exists segments_route_to_stop_direction_idx
  on segments (route_id, to_stop_id, direction_id, id);

create index if not exists scheduled_headways_route_stop_lookup_idx
  on scheduled_headways (route_id, stop_id, service_id, direction_id, time_bin_start)
  where scheduled_headway_min is not null;

create index if not exists gtfs_calendar_dates_date_exception_idx
  on gtfs_calendar_dates (date, exception_type, service_id);

create index if not exists headways_arrival_id_idx
  on headways (arrival_time, id);

create index if not exists headways_enriched_arrival_time_idx
  on headways_enriched (arrival_time);

create index if not exists route_bunching_stats_filter_idx
  on route_bunching_stats (service_id, time_of_day_bucket, route_id);

create index if not exists segment_bunching_stats_filter_idx
  on segment_bunching_stats (route_id, service_id, time_of_day_bucket, segment_id);

create index if not exists route_hourly_bunching_stats_filter_idx
  on route_hourly_bunching_stats (route_id, service_id, hour_of_day);

-- Precompute the route-level scheduled-headway fallback that enrichment previously
-- recalculated inside a lateral subquery for every source headway.
create table if not exists scheduled_headway_route_fallback (
  route_id text not null,
  service_id text not null,
  direction_id int,
  time_bin_start time not null,
  scheduled_headway_min double precision not null
);

create unique index if not exists scheduled_headway_route_fallback_unique_idx
  on scheduled_headway_route_fallback (route_id, service_id, coalesce(direction_id, -1), time_bin_start);

create index if not exists scheduled_headway_route_fallback_lookup_idx
  on scheduled_headway_route_fallback (route_id, service_id, direction_id, time_bin_start);

create or replace function refresh_scheduled_headway_route_fallback()
returns void
language plpgsql
as $$
begin
  delete from scheduled_headway_route_fallback;

  insert into scheduled_headway_route_fallback (
    route_id,
    service_id,
    direction_id,
    time_bin_start,
    scheduled_headway_min
  )
  select
    route_id,
    service_id,
    direction_id,
    time_bin_start,
    avg(scheduled_headway_min) as scheduled_headway_min
  from scheduled_headways
  where scheduled_headway_min is not null
    and service_id is not null
    and time_bin_start is not null
  group by route_id, service_id, direction_id, time_bin_start;
end;
$$;

select refresh_scheduled_headway_route_fallback();

create or replace function enrich_headways_batch_safe(p_limit integer default 10000)
returns integer
language plpgsql
as $$
declare
  v_rows integer;
begin
  with source_headways as (
    select h.*
    from headways h
    where not exists (
      select 1
      from headways_enriched he
      where he.headway_id = h.id
    )
    order by h.arrival_time, h.id
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
      headway_id,
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
      h.id as headway_id,
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
          and sh.scheduled_headway_min is not null

        union all

        select
          1 as scope_rank,
          fallback.service_id,
          fallback.direction_id,
          fallback.time_bin_start,
          fallback.scheduled_headway_min
        from scheduled_headway_route_fallback fallback
        where fallback.route_id = h.route_id
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
            and (array[c.sunday, c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday])
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
    on conflict (headway_id) do nothing
    returning 1
  )
  select count(*) into v_rows from inserted;

  return coalesce(v_rows, 0);
end;
$$;

-- Refresh all stats from one materialized recent window instead of scanning
-- headways_enriched once per aggregate table.
create or replace function refresh_bunching_stats(p_days integer default 30)
returns void
language plpgsql
as $$
begin
  drop table if exists pg_temp.recent_headways_enriched;

  create temp table recent_headways_enriched on commit drop as
  select
    he.route_id,
    coalesce(he.direction_id, -1) as direction_id,
    he.segment_id,
    coalesce(he.service_id, 'unknown') as service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time)) as time_of_day_bucket,
    extract(hour from (he.arrival_time at time zone 'America/Chicago'))::int as hour_of_day,
    he.bunched,
    he.super_bunched,
    he.hw_ratio,
    he.scheduled_headway_min,
    he.actual_headway_min
  from (
    select
      route_id,
      direction_id,
      segment_id,
      service_id,
      time_of_day_bucket,
      arrival_time,
      bunched,
      super_bunched,
      hw_ratio,
      scheduled_headway_min,
      actual_headway_min
    from headways_enriched
    where arrival_time >= now() - (p_days * interval '1 day')
  ) he;

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
    median_scheduled_headway,
    median_actual_headway
  )
  select
    route_id,
    direction_id,
    service_id,
    time_of_day_bucket,
    count(*) as total_headways,
    count(*) filter (where bunched) as bunched_headways,
    count(*) filter (where super_bunched) as super_bunched_headways,
    avg((bunched)::int)::float as bunching_rate,
    avg(hw_ratio) as avg_hw_ratio,
    percentile_cont(0.5) within group (order by scheduled_headway_min) as median_scheduled_headway,
    percentile_cont(0.5) within group (order by actual_headway_min) as median_actual_headway
  from recent_headways_enriched
  group by route_id, direction_id, service_id, time_of_day_bucket;

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
    time_of_day_bucket,
    count(*) as total_headways,
    count(*) filter (where bunched) as bunched_headways,
    avg((bunched)::int)::float as bunching_rate
  from recent_headways_enriched
  group by segment_id, route_id, direction_id, service_id, time_of_day_bucket;

  delete from route_hourly_bunching_stats;

  insert into route_hourly_bunching_stats (
    route_id,
    service_id,
    hour_of_day,
    total_headways,
    bunched_headways,
    computed_at,
    window_days
  )
  select
    route_id,
    service_id,
    hour_of_day,
    count(*)::int as total_headways,
    count(*) filter (where bunched)::int as bunched_headways,
    now() as computed_at,
    p_days as window_days
  from recent_headways_enriched
  group by route_id, service_id, hour_of_day;
end;
$$;

create or replace function refresh_route_bunching_stats(p_days integer default 30)
returns void
language plpgsql
as $$
begin
  perform refresh_bunching_stats(p_days);
end;
$$;

create or replace function refresh_segment_bunching_stats(p_days integer default 30)
returns void
language plpgsql
as $$
begin
  perform refresh_bunching_stats(p_days);
end;
$$;

create or replace function refresh_route_hourly_bunching_stats(p_days integer default 30)
returns void
language plpgsql
as $$
begin
  perform refresh_bunching_stats(p_days);
end;
$$;
