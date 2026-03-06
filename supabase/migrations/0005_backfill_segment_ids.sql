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
        select 1
        from segments s
        where s.route_id = h.route_id
          and s.to_stop_id = h.stop_id
      )
    order by h.id
    limit p_limit
  ),
  updated as (
    update headways h
    set segment_id = (
      select s.id
      from segments s
      where s.route_id = h.route_id
        and s.to_stop_id = h.stop_id
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

create or replace function backfill_headways_enriched_segment_ids(p_limit integer default 10000)
returns integer
language plpgsql
as $$
declare
  v_rows integer;
begin
  with batch as (
    select he.id, h.segment_id
    from headways_enriched he
    join headways h
      on h.route_id = he.route_id
     and h.stop_id = he.stop_id
     and h.arrival_time = he.arrival_time
     and h.headway_min = he.actual_headway_min
    where he.segment_id is null
      and h.segment_id is not null
    order by he.id
    limit p_limit
  ),
  updated as (
    update headways_enriched he
    set segment_id = b.segment_id
    from batch b
    where he.id = b.id
    returning 1
  )
  select count(*) into v_rows from updated;

  return coalesce(v_rows, 0);
end;
$$;
