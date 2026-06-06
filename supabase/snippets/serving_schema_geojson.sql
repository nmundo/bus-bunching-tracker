-- Minimal "serving" schema for a small Supabase Postgres instance (no PostGIS required).
-- Intended for daily bulk publish from a larger local "warehouse" database.

create table if not exists gtfs_routes (
  route_id text primary key,
  route_short_name text,
  route_long_name text
);

create table if not exists gtfs_stops (
  stop_id text primary key,
  stop_name text
);

create table if not exists gtfs_calendar (
  service_id text primary key,
  monday int,
  tuesday int,
  wednesday int,
  thursday int,
  friday int,
  saturday int,
  sunday int,
  start_date date,
  end_date date
);

create table if not exists segments (
  id uuid primary key,
  route_id text not null,
  direction_id int,
  from_stop_id text,
  to_stop_id text,
  geometry jsonb not null
);

create index if not exists segments_route_id_idx on segments (route_id);

create table if not exists route_bunching_stats (
  route_id text not null,
  direction_id int not null,
  service_id text not null,
  time_of_day_bucket text not null,
  total_headways int,
  bunched_headways int,
  super_bunched_headways int,
  bunching_rate double precision,
  avg_hw_ratio double precision,
  median_scheduled_headway double precision,
  median_actual_headway double precision,
  gapped_headways int,
  observed_wait_min double precision,
  scheduled_wait_min double precision,
  excess_wait_min double precision,
  headway_cv double precision,
  primary key (route_id, direction_id, service_id, time_of_day_bucket)
);

create index if not exists route_bunching_stats_route_idx on route_bunching_stats (route_id);

create table if not exists segment_bunching_stats (
  segment_id uuid not null,
  route_id text not null,
  direction_id int not null default -1,
  service_id text not null,
  time_of_day_bucket text not null,
  total_headways int,
  bunched_headways int,
  bunching_rate double precision,
  primary key (segment_id, service_id, time_of_day_bucket)
);

create index if not exists segment_bunching_stats_route_bucket_idx
  on segment_bunching_stats (route_id, time_of_day_bucket);

create table if not exists route_hourly_bunching_stats (
  route_id text not null,
  service_id text not null,
  hour_of_day int not null check (hour_of_day >= 0 and hour_of_day <= 23),
  total_headways int not null,
  bunched_headways int not null,
  computed_at timestamptz not null,
  window_days int not null,
  primary key (route_id, service_id, hour_of_day)
);

create table if not exists route_daily_bunching_stats (
  route_id text not null,
  service_id text not null,
  stat_date date not null,
  total_headways int not null,
  bunched_headways int not null,
  bunching_rate double precision,
  excess_wait_min double precision,
  headway_cv double precision,
  computed_at timestamptz not null,
  primary key (route_id, service_id, stat_date)
);

create index if not exists route_daily_bunching_stats_route_idx
  on route_daily_bunching_stats (route_id, stat_date);

create table if not exists route_direction_labels (
  route_id    text not null,
  direction_id int not null,
  dir         text not null,
  primary key (route_id, direction_id)
);

create table if not exists publish_meta (
  id int primary key check (id = 1),
  last_published_at timestamptz not null,
  window_days int not null,
  max_observed_arrival_time timestamptz
);
