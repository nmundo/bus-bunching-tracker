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
