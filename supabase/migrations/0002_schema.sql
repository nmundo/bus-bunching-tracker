-- GTFS static tables
create table if not exists gtfs_routes (
  route_id text primary key,
  agency_id text,
  route_short_name text,
  route_long_name text,
  route_type int
);

create index if not exists gtfs_routes_short_name_idx on gtfs_routes (route_short_name);

create table if not exists gtfs_stops (
  stop_id text primary key,
  stop_name text,
  stop_lat double precision,
  stop_lon double precision,
  geom geometry(point, 4326)
);

create index if not exists gtfs_stops_geom_idx on gtfs_stops using gist (geom);

create table if not exists gtfs_trips (
  trip_id text primary key,
  route_id text references gtfs_routes(route_id),
  service_id text,
  shape_id text,
  direction_id int
);

create table if not exists gtfs_stop_times (
  trip_id text references gtfs_trips(trip_id),
  arrival_time time,
  departure_time time,
  stop_id text references gtfs_stops(stop_id),
  stop_sequence int,
  primary key (trip_id, stop_sequence)
);

create index if not exists gtfs_stop_times_stop_departure_idx on gtfs_stop_times (stop_id, departure_time);

create table if not exists gtfs_shapes (
  shape_id text,
  shape_pt_lat double precision,
  shape_pt_lon double precision,
  shape_pt_sequence int,
  primary key (shape_id, shape_pt_sequence)
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

create table if not exists gtfs_calendar_dates (
  service_id text references gtfs_calendar(service_id),
  date date,
  exception_type int,
  primary key (service_id, date)
);

create table if not exists gtfs_frequencies (
  id bigserial primary key,
  trip_id text references gtfs_trips(trip_id),
  start_time time,
  end_time time,
  headway_secs int
);

create unique index if not exists gtfs_frequencies_unique_idx
  on gtfs_frequencies (trip_id, start_time, end_time);

-- Canonical sequences and segments
create table if not exists segments (
  id uuid primary key default gen_random_uuid(),
  route_id text references gtfs_routes(route_id),
  direction_id int,
  from_stop_id text references gtfs_stops(stop_id),
  to_stop_id text references gtfs_stops(stop_id),
  geom geometry(linestring, 4326)
);

create index if not exists segments_geom_idx on segments using gist (geom);

create table if not exists route_stop_sequences (
  id uuid primary key default gen_random_uuid(),
  route_id text references gtfs_routes(route_id),
  direction_id int,
  stop_sequence int,
  stop_id text references gtfs_stops(stop_id),
  shape_id text,
  cumulative_distance_m double precision,
  segment_id uuid references segments(id)
);

create unique index if not exists route_stop_sequences_unique_idx
  on route_stop_sequences (route_id, direction_id, stop_sequence);

-- Scheduled headways
create table if not exists scheduled_headways (
  id uuid primary key default gen_random_uuid(),
  route_id text references gtfs_routes(route_id),
  direction_id int,
  stop_id text references gtfs_stops(stop_id),
  service_id text,
  time_bin_start time,
  time_bin_end time,
  scheduled_headway_min double precision
);

create unique index if not exists scheduled_headways_unique_idx
  on scheduled_headways (route_id, direction_id, stop_id, service_id, time_bin_start);

-- Bus Tracker reference data
create table if not exists bt_routes (
  rt text primary key,
  rtnm text,
  rtclr text
);

create table if not exists bt_stops (
  stpid text primary key,
  stpnm text,
  lat double precision,
  lon double precision,
  geom geometry(point, 4326),
  rt text references bt_routes(rt),
  dir text
);

create index if not exists bt_stops_geom_idx on bt_stops using gist (geom);

create table if not exists bt_patterns (
  pid text primary key,
  rt text references bt_routes(rt),
  dir text,
  geom geometry(linestring, 4326)
);

create table if not exists bt_pattern_stops (
  id uuid primary key default gen_random_uuid(),
  pid text references bt_patterns(pid),
  seq int,
  stpid text references bt_stops(stpid)
);

create table if not exists route_map (
  rt text primary key references bt_routes(rt),
  gtfs_route_id text references gtfs_routes(route_id)
);

create table if not exists stop_map (
  stpid text primary key references bt_stops(stpid),
  gtfs_stop_id text references gtfs_stops(stop_id)
);

-- Raw vehicle positions
create table if not exists bus_positions (
  id bigserial primary key,
  vid text,
  rt text,
  des text,
  pid text references bt_patterns(pid),
  lat double precision,
  lon double precision,
  geom geometry(point, 4326),
  pdist_feet double precision,
  tmstmp timestamptz,
  tatripid text,
  tablockid text
);

create index if not exists bus_positions_rt_tmstmp_idx on bus_positions (rt, tmstmp);
create index if not exists bus_positions_geom_idx on bus_positions using gist (geom);

-- Derived arrivals and headways
create table if not exists stop_arrivals (
  id bigserial primary key,
  route_id text,
  direction_id int,
  stop_id text references gtfs_stops(stop_id),
  vid text,
  rt text,
  pid text,
  arrival_time timestamptz,
  pdist_feet double precision
);

create index if not exists stop_arrivals_idx
  on stop_arrivals (route_id, direction_id, stop_id, arrival_time);

create table if not exists headways (
  id bigserial primary key,
  route_id text,
  direction_id int,
  stop_id text references gtfs_stops(stop_id),
  prev_vid text,
  curr_vid text,
  arrival_time timestamptz,
  headway_min double precision,
  segment_id uuid references segments(id)
);

create index if not exists headways_idx
  on headways (route_id, direction_id, stop_id, arrival_time);

create unique index if not exists headways_unique_idx
  on headways (route_id, direction_id, stop_id, arrival_time, curr_vid);

create table if not exists headways_enriched (
  id bigserial primary key,
  route_id text,
  direction_id int,
  stop_id text,
  segment_id uuid,
  arrival_time timestamptz,
  service_id text,
  time_bin_start time,
  actual_headway_min double precision,
  scheduled_headway_min double precision,
  hw_ratio double precision,
  bunched boolean,
  super_bunched boolean,
  gapped boolean
);

create index if not exists headways_enriched_route_time_idx on headways_enriched (route_id, arrival_time);
create index if not exists headways_enriched_segment_time_idx on headways_enriched (segment_id, arrival_time);

-- Aggregated stats tables
create table if not exists route_bunching_stats (
  id uuid primary key default gen_random_uuid(),
  route_id text,
  direction_id int,
  service_id text,
  time_of_day_bucket text,
  total_headways int,
  bunched_headways int,
  super_bunched_headways int,
  bunching_rate double precision,
  avg_hw_ratio double precision,
  median_actual_headway double precision
);

create table if not exists segment_bunching_stats (
  id uuid primary key default gen_random_uuid(),
  segment_id uuid references segments(id),
  route_id text,
  direction_id int,
  service_id text,
  time_of_day_bucket text,
  total_headways int,
  bunched_headways int,
  bunching_rate double precision
);

create index if not exists segment_bunching_stats_route_bucket_idx
  on segment_bunching_stats (route_id, time_of_day_bucket);

-- Job state for incremental processing
create table if not exists job_state (
  id text primary key,
  watermark timestamptz
);
