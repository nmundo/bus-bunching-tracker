-- Hourly route buckets for UI charting without scanning headways_enriched
create table if not exists route_hourly_bunching_stats (
  route_id text not null,
  service_id text not null,
  hour_of_day int not null check (hour_of_day >= 0 and hour_of_day <= 23),
  total_headways int not null,
  bunched_headways int not null,
  computed_at timestamptz not null default now(),
  window_days int not null,
  primary key (route_id, service_id, hour_of_day)
);

create index if not exists route_hourly_bunching_stats_route_idx
  on route_hourly_bunching_stats (route_id);

create or replace function refresh_route_hourly_bunching_stats(p_days integer default 30)
returns void
language plpgsql
as $$
begin
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
    he.route_id,
    coalesce(he.service_id, 'unknown') as service_id,
    extract(hour from (he.arrival_time at time zone 'America/Chicago'))::int as hour_of_day,
    count(*)::int as total_headways,
    count(*) filter (where he.bunched)::int as bunched_headways,
    now() as computed_at,
    p_days as window_days
  from headways_enriched he
  where he.arrival_time >= now() - (p_days * interval '1 day')
  group by
    he.route_id,
    coalesce(he.service_id, 'unknown'),
    extract(hour from (he.arrival_time at time zone 'America/Chicago'))::int;
end;
$$;

-- Store a GeoJSON copy of segment geometry for serving-mode DBs without PostGIS.
alter table segments
  add column if not exists geometry jsonb;

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

update segments
set geometry = st_asgeojson(geom)::jsonb
where geom is not null
  and geometry is null;

