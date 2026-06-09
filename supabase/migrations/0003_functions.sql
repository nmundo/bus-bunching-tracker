-- Helper: resolve the active service_id for a given calendar date
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
      and (array[c.sunday, c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday])
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

-- Helper: classify a timestamp into a named time-of-day bucket (Chicago local time)
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

-- Trigger: keep segments.geometry (jsonb) in sync with segments.geom (postgis)
create or replace function segments_set_geometry_from_geom()
returns trigger
language plpgsql
as $$
begin
  if new.geom is not null then
    new.geometry := st_asgeojson(new.geom)::jsonb;
  end if;
  return new;
end;
$$;

drop trigger if exists segments_set_geometry_from_geom_trigger on segments;
create trigger segments_set_geometry_from_geom_trigger
before insert or update of geom on segments
for each row
execute function segments_set_geometry_from_geom();

-- Enrichment: batched version using the precomputed route-level fallback table
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
      select 1 from headways_enriched he where he.headway_id = h.id
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

-- Backfill: assign segment_id to headways rows that are missing one
create or replace function backfill_headways_segment_ids(p_limit integer default 10000)
returns integer
language plpgsql
as $$
declare
  v_rows integer;
begin
  with batch as (
    select h.id
    from headways h
    where h.segment_id is null
      and exists (
        select 1 from segments s
        where s.route_id = h.route_id and s.to_stop_id = h.stop_id
      )
    order by h.id
    limit p_limit
  ),
  updated as (
    update headways h
    set segment_id = (
      select s.id
      from segments s
      where s.route_id = h.route_id and s.to_stop_id = h.stop_id
      order by
        case
          when s.direction_id = h.direction_id then 0
          when s.direction_id is null then 2
          else 1
        end,
        s.id
      limit 1
    )
    where h.id in (select id from batch)
    returning 1
  )
  select count(*) into v_rows from updated;

  return coalesce(v_rows, 0);
end;
$$;

-- Backfill: assign segment_id to headways_enriched rows that are missing one
create or replace function backfill_headways_enriched_segment_ids(p_limit integer default 10000)
returns integer
language plpgsql
as $$
declare
  v_rows integer;
begin
  with batch as (
    select he.id
    from headways_enriched he
    where he.segment_id is null
    order by he.id
    limit p_limit
  ),
  matched as (
    select
      b.id,
      (
        select h.segment_id
        from headways_enriched he2
        join headways h
          on h.route_id = he2.route_id
         and h.stop_id = he2.stop_id
         and h.arrival_time = he2.arrival_time
        where he2.id = b.id
          and h.segment_id is not null
        order by
          abs(coalesce(h.headway_min, 0) - coalesce(he2.actual_headway_min, 0)),
          h.id
        limit 1
      ) as segment_id
    from batch b
  ),
  updated as (
    update headways_enriched he
    set segment_id = m.segment_id
    from matched m
    where he.id = m.id
      and m.segment_id is not null
    returning 1
  )
  select count(*) into v_rows from updated;

  return coalesce(v_rows, 0);
end;
$$;

-- Stats: rebuild scheduled_headway_route_fallback from scheduled_headways
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

-- Stats: rebuild all three aggregation tables from a single temp-table scan
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
    he.gapped,
    he.hw_ratio,
    he.scheduled_headway_min,
    he.actual_headway_min,
    -- "analyzable": has a scheduled baseline and is within the sanity cap.
    -- MAX_ANALYZABLE_HEADWAY_MIN = 180 drops cross-service-gap / GPS-glitch
    -- headways that would otherwise dominate variance and the wait integrals.
    (he.scheduled_headway_min is not null and he.actual_headway_min <= 180) as analyzable
  from (
    select
      route_id, direction_id, segment_id, service_id, time_of_day_bucket,
      arrival_time, bunched, super_bunched, gapped, hw_ratio,
      scheduled_headway_min, actual_headway_min
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
    gapped_headways,
    bunching_rate,
    avg_hw_ratio,
    median_scheduled_headway,
    median_actual_headway,
    observed_wait_min,
    scheduled_wait_min,
    excess_wait_min,
    headway_cv,
    analyzable_headways,
    sum_actual_hw,
    sum_actual_hw_sq,
    sum_sched_hw,
    sum_sched_hw_sq
  )
  select
    route_id,
    direction_id,
    service_id,
    time_of_day_bucket,
    count(*) as total_headways,
    -- bunched is a strict subset of analyzable; gapped is capped to analyzable so
    -- pathological long gaps don't count. Both share the analyzable denominator.
    count(*) filter (where bunched) as bunched_headways,
    count(*) filter (where super_bunched) as super_bunched_headways,
    count(*) filter (where gapped and analyzable) as gapped_headways,
    (count(*) filter (where bunched))::float / nullif(count(*) filter (where analyzable), 0) as bunching_rate,
    avg(hw_ratio) as avg_hw_ratio,
    percentile_cont(0.5) within group (order by scheduled_headway_min) filter (where analyzable) as median_scheduled_headway,
    percentile_cont(0.5) within group (order by actual_headway_min) filter (where analyzable) as median_actual_headway,
    -- Mean passenger wait for random arrivals = E[H^2] / (2*E[H]), over analyzable
    -- rows only since "excess" needs a scheduled baseline.
    sum(actual_headway_min * actual_headway_min) filter (where analyzable)
      / nullif(2 * sum(actual_headway_min) filter (where analyzable), 0)
      as observed_wait_min,
    sum(scheduled_headway_min * scheduled_headway_min) filter (where analyzable)
      / nullif(2 * sum(scheduled_headway_min) filter (where analyzable), 0)
      as scheduled_wait_min,
    (sum(actual_headway_min * actual_headway_min) filter (where analyzable)
       / nullif(2 * sum(actual_headway_min) filter (where analyzable), 0))
    - (sum(scheduled_headway_min * scheduled_headway_min) filter (where analyzable)
       / nullif(2 * sum(scheduled_headway_min) filter (where analyzable), 0))
      as excess_wait_min,
    stddev_samp(actual_headway_min) filter (where analyzable)
      / nullif(avg(actual_headway_min) filter (where analyzable), 0) as headway_cv,
    -- Sufficient statistics for exact re-aggregation downstream.
    count(*) filter (where analyzable) as analyzable_headways,
    coalesce(sum(actual_headway_min) filter (where analyzable), 0) as sum_actual_hw,
    coalesce(sum(actual_headway_min * actual_headway_min) filter (where analyzable), 0) as sum_actual_hw_sq,
    coalesce(sum(scheduled_headway_min) filter (where analyzable), 0) as sum_sched_hw,
    coalesce(sum(scheduled_headway_min * scheduled_headway_min) filter (where analyzable), 0) as sum_sched_hw_sq
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

-- Stats: snapshot one local (Chicago) calendar day of enriched headways into the
-- daily trend table.  Defaults to "yesterday" so it runs against a complete day.
-- Unlike refresh_bunching_stats this only replaces the target day, preserving
-- history that has aged out of the rolling enrichment window.
create or replace function snapshot_daily_bunching_stats(p_date date default null)
returns integer
language plpgsql
as $$
declare
  v_date date := coalesce(p_date, ((now() at time zone 'America/Chicago')::date - 1));
  v_rows integer;
begin
  delete from route_daily_bunching_stats where stat_date = v_date;

  insert into route_daily_bunching_stats (
    route_id, service_id, stat_date,
    total_headways, bunched_headways, bunching_rate,
    excess_wait_min, headway_cv,
    analyzable_headways, sum_actual_hw, sum_actual_hw_sq, sum_sched_hw, sum_sched_hw_sq,
    computed_at
  )
  select
    he.route_id,
    coalesce(he.service_id, 'unknown') as service_id,
    v_date as stat_date,
    count(*) as total_headways,
    count(*) filter (where he.bunched) as bunched_headways,
    (count(*) filter (where he.bunched))::float / nullif(count(*) filter (where a.analyzable), 0) as bunching_rate,
    (sum(he.actual_headway_min * he.actual_headway_min) filter (where a.analyzable)
       / nullif(2 * sum(he.actual_headway_min) filter (where a.analyzable), 0))
    - (sum(he.scheduled_headway_min * he.scheduled_headway_min) filter (where a.analyzable)
       / nullif(2 * sum(he.scheduled_headway_min) filter (where a.analyzable), 0))
      as excess_wait_min,
    stddev_samp(he.actual_headway_min) filter (where a.analyzable)
      / nullif(avg(he.actual_headway_min) filter (where a.analyzable), 0) as headway_cv,
    count(*) filter (where a.analyzable) as analyzable_headways,
    coalesce(sum(he.actual_headway_min) filter (where a.analyzable), 0) as sum_actual_hw,
    coalesce(sum(he.actual_headway_min * he.actual_headway_min) filter (where a.analyzable), 0) as sum_actual_hw_sq,
    coalesce(sum(he.scheduled_headway_min) filter (where a.analyzable), 0) as sum_sched_hw,
    coalesce(sum(he.scheduled_headway_min * he.scheduled_headway_min) filter (where a.analyzable), 0) as sum_sched_hw_sq,
    now()
  from headways_enriched he
  cross join lateral (
    select (he.scheduled_headway_min is not null and he.actual_headway_min <= 180) as analyzable
  ) a
  where (he.arrival_time at time zone 'America/Chicago')::date = v_date
  group by he.route_id, coalesce(he.service_id, 'unknown');

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

-- Lookup: rebuild route_direction_labels from get_route_directions() for all routes.
-- Called after every GTFS import (once route_stop_sequences is populated).
create or replace function refresh_route_direction_labels()
returns void
language plpgsql
as $$
begin
  delete from route_direction_labels;

  insert into route_direction_labels (route_id, direction_id, dir)
  select r.route_id, d.direction_id, d.dir
  from gtfs_routes r
  cross join lateral get_route_directions(r.route_id) d;
end;
$$;

-- Lookup: return the CTA direction label (e.g. "Northbound") for each GTFS direction_id of a route.
-- Joins through the first terminal stop of each direction → stop_map → bt_pattern_stops → bt_patterns,
-- which is the only reliable path because shared stops in stop_map can carry the wrong dir label.
create or replace function get_route_directions(p_route_id text)
returns table (direction_id int, dir text)
language sql
stable
as $$
  select distinct on (rss_first.direction_id)
    rss_first.direction_id,
    bp.dir
  from (
    select distinct on (direction_id) direction_id, stop_id
    from route_stop_sequences
    where route_id = p_route_id
    order by direction_id, stop_sequence
  ) rss_first
  join stop_map sm on sm.gtfs_stop_id = rss_first.stop_id
  join bt_pattern_stops bps on bps.stpid = sm.stpid and bps.seq <= 3
  join bt_patterns bp on bp.pid = bps.pid
  join route_map rm on rm.rt = bp.rt and rm.gtfs_route_id = p_route_id
  order by rss_first.direction_id;
$$;

-- Stats: wrappers so callers can refresh individual tables without knowing the internals
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
