alter table if exists route_bunching_stats
  add column if not exists gapped_headways int;

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
    gapped_headways,
    bunching_rate,
    avg_hw_ratio,
    median_scheduled_headway,
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
    count(*) filter (where gapped) as gapped_headways,
    avg((bunched)::int)::float as bunching_rate,
    avg(hw_ratio) as avg_hw_ratio,
    percentile_cont(0.5) within group (order by scheduled_headway_min) as median_scheduled_headway,
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
