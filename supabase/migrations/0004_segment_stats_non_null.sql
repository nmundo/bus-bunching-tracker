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
    and segment_id is not null
  group by
    segment_id,
    route_id,
    direction_id,
    service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time));
end;
$$;
