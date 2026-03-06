alter table if exists headways_enriched
  add column if not exists headway_id bigint references headways(id);

with ranked_headways as (
  select
    h.id as headway_id,
    h.route_id,
    h.direction_id,
    h.stop_id,
    h.arrival_time,
    h.headway_min,
    row_number() over (
      partition by h.route_id, h.direction_id, h.stop_id, h.arrival_time, h.headway_min
      order by h.id
    ) as rn
  from headways h
),
ranked_enriched as (
  select
    he.id as enriched_id,
    he.route_id,
    he.direction_id,
    he.stop_id,
    he.arrival_time,
    he.actual_headway_min,
    row_number() over (
      partition by he.route_id, he.direction_id, he.stop_id, he.arrival_time, he.actual_headway_min
      order by he.id
    ) as rn
  from headways_enriched he
  where he.headway_id is null
),
matched as (
  select
    re.enriched_id,
    rh.headway_id
  from ranked_enriched re
  join ranked_headways rh
    on rh.route_id = re.route_id
   and rh.direction_id is not distinct from re.direction_id
   and rh.stop_id = re.stop_id
   and rh.arrival_time = re.arrival_time
   and rh.headway_min is not distinct from re.actual_headway_min
   and rh.rn = re.rn
)
update headways_enriched he
set headway_id = matched.headway_id
from matched
where he.id = matched.enriched_id
  and he.headway_id is null;

do $$
begin
  if exists (select 1 from headways_enriched where headway_id is null) then
    raise exception 'headways_enriched contains rows without headway_id after backfill';
  end if;
end;
$$;

alter table headways_enriched
  alter column headway_id set not null;

create unique index if not exists headways_enriched_headway_id_uidx
  on headways_enriched (headway_id);

create or replace function enrich_headways()
returns void
language plpgsql
as $$
begin
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
    on he.headway_id = h.id
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
  where he.id is null
  on conflict (headway_id) do nothing;
end;
$$;

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
    left join headways_enriched he
      on he.headway_id = h.id
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
