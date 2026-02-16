You are a senior full‑stack engineer. Build a production‑ready web app to analyze CTA bus bunching using:
• Supabase (Postgres + PostGIS) as the database and auth
• Node.js (TypeScript) for backend workers + API (SvelteKit server routes or a separate worker service are both fine; pick one and be consistent)
• Svelte (SvelteKit) for the frontend
• CTA GTFS static data and CTA Bus Tracker API for real‑time data
The app should:
• Continuously ingest CTA Bus Tracker vehicle locations
• Derive actual headways between buses at stops along routes
• Compare actual headways to scheduled headways from GTFS to identify bunching
• Aggregate and visualize which routes, times of day, and route segments have the worst bunching
I care most about: correctness of the data pipeline, clear structure, and maintainability.
Follow these instructions step by step.

1.  Supabase & PostGIS setup 1. Initialize a new Supabase project (assume project is already created and CLI is logged in). 2. Enable PostGIS in the database using SQL (or via dashboard):
    `create extension if not exists postgis with schema extensions;`
2.  Use SQL migrations (via  supabase db push  /  generate ) instead of ad‑hoc SQL so schema is version‑controlled.
3.  Database schema design
    Create SQL migrations to define the schema below. Use snake_case,  uuid  PKs where appropriate, and proper foreign keys. Add indexes where indicated.
    2.1. GTFS static schema (import only, read‑only)
    Tables:
    •  gtfs_routes 
    •  route_id text primary key 
    •  agency_id text 
    •  route_short_name text 
    •  route_long_name text 
    •  route_type int 
    • Index on  route_short_name 
    •  gtfs_stops 
    •  stop_id text primary key 
    •  stop_name text 
    •  stop_lat double precision 
    •  stop_lon double precision 
    •  geom geometry(point, 4326) 
    • Index on  geom  (GiST)
    •  gtfs_trips 
    •  trip_id text primary key 
    •  route_id text references gtfs_routes(route_id) 
    •  service_id text 
    •  shape_id text 
    •  direction_id int 
    •  gtfs_stop_times 
    •  trip_id text references gtfs_trips(trip_id) 
    •  arrival_time time 
    •  departure_time time 
    •  stop_id text references gtfs_stops(stop_id) 
    •  stop_sequence int 
    • Composite index on  (stop_id, departure_time) 
    •  gtfs_shapes 
    •  shape_id text 
    •  shape_pt_lat double precision 
    •  shape_pt_lon double precision 
    •  shape_pt_sequence int 
    •  gtfs_calendar 
    • Standard GTFS fields ( service_id ,  monday … sunday ,  start_date ,  end_date )
    •  gtfs_calendar_dates 
    • Standard GTFS fields
    •  gtfs_frequencies  (if needed)
    •  trip_id ,  start_time ,  end_time ,  headway_secs 
    2.2. Canonical route sequences and segments
    Derived tables (you will populate via SQL scripts / Node ETL):
    •  route_stop_sequences 
    •  id uuid primary key default gen_random_uuid() 
    •  route_id text references gtfs_routes(route_id) 
    •  direction_id int 
    •  stop_sequence int 
    •  stop_id text references gtfs_stops(stop_id) 
    •  shape_id text 
    •  cumulative_distance_m double precision  (distance along shape from start)
    •  segment_id uuid  (FK to segments table)
    • Unique index on  (route_id, direction_id, stop_sequence) 
    •  segments 
    •  id uuid primary key default gen_random_uuid() 
    •  route_id text 
    •  direction_id int 
    •  from_stop_id text references gtfs_stops(stop_id) 
    •  to_stop_id text references gtfs_stops(stop_id) 
    •  geom geometry(linestring, 4326) 
    • GiST index on  geom 
    2.3. Scheduled headways (precomputed)
    •  scheduled_headways 
    •  id uuid primary key default gen_random_uuid() 
    •  route_id text references gtfs_routes(route_id) 
    •  direction_id int 
    •  stop_id text references gtfs_stops(stop_id) 
    •  service_id text 
    •  time_bin_start time  (e.g., 07:00:00)
    •  time_bin_end time 
    •  scheduled_headway_min double precision 
    • Unique index on  (route_id, direction_id, stop_id, service_id, time_bin_start) 
    2.4. Bus Tracker reference data
    Populated from Bus Tracker endpoints:
    •  bt_routes 
    •  rt text primary key 
    •  rtnm text 
    •  rtclr text 
    •  bt_stops 
    •  stpid text primary key 
    •  stpnm text 
    •  lat double precision 
    •  lon double precision 
    •  geom geometry(point, 4326) 
    •  rt text references bt_routes(rt) 
    •  dir text 
    • GiST index on  geom 
    •  bt_patterns 
    •  pid text primary key 
    •  rt text references bt_routes(rt) 
    •  dir text 
    •  geom geometry(linestring, 4326) 
    •  bt_pattern_stops 
    •  id uuid primary key default gen_random_uuid() 
    •  pid text references bt_patterns(pid) 
    •  seq int 
    •  stpid text references bt_stops(stpid) 
    • Mapping tables (if needed):
    •  route_map(rt text primary key, gtfs_route_id text references gtfs_routes(route_id)) 
    •  stop_map(stpid text primary key, gtfs_stop_id text references gtfs_stops(stop_id)) 
    2.5. Raw vehicle positions (Bus Tracker)
    •  bus_positions 
    •  id bigserial primary key 
    •  vid text  – vehicle ID
    •  rt text  – route
    •  des text 
    •  pid text references bt_patterns(pid) 
    •  lat double precision 
    •  lon double precision 
    •  geom geometry(point, 4326) 
    •  pdist_feet double precision  – pdist from API
    •  tmstmp timestamptz  – parsed Bus Tracker  tmstmp 
    •  tatripid text 
    •  tablockid text 
    • Index on  (rt, tmstmp) 
    • GiST index on  geom 
    • Consider table partitioning by date ( tmstmp ) if supported in Supabase migrations.
    2.6. Derived arrivals and headways
    •  stop_arrivals 
    •  id bigserial primary key 
    •  route_id text 
    •  direction_id int 
    •  stop_id text references gtfs_stops(stop_id) 
    •  vid text 
    •  rt text 
    •  pid text 
    •  arrival_time timestamptz 
    •  pdist_feet double precision 
    • Index on  (route_id, direction_id, stop_id, arrival_time) 
    •  headways 
    •  id bigserial primary key 
    •  route_id text 
    •  direction_id int 
    •  stop_id text references gtfs_stops(stop_id) 
    •  prev_vid text 
    •  curr_vid text 
    •  arrival_time timestamptz  – time of  curr_vid 
    •  headway_min double precision 
    •  segment_id uuid references segments(id) 
    • Index on  (route_id, direction_id, stop_id, arrival_time) 
    •  headways_enriched 
    •  id bigserial primary key 
    •  route_id text 
    •  direction_id int 
    •  stop_id text 
    •  segment_id uuid 
    •  arrival_time timestamptz 
    •  service_id text 
    •  time_bin_start time 
    •  actual_headway_min double precision 
    •  scheduled_headway_min double precision 
    •  hw_ratio double precision 
    •  bunched boolean 
    •  super_bunched boolean 
    •  gapped boolean 
    • Index on  (route_id, arrival_time) 
    • Index on  (segment_id, arrival_time) 
    2.7. Aggregated stats
    Create materialized views or physical tables for fast queries:
    •  route_bunching_stats 
    •  id uuid primary key default gen_random_uuid() 
    •  route_id text 
    •  direction_id int 
    •  service_id text 
    •  time_of_day_bucket text  – e.g. “AM_peak”, “Midday”
    •  total_headways int 
    •  bunched_headways int 
    •  super_bunched_headways int 
    •  bunching_rate double precision  – bunched / total
    •  avg_hw_ratio double precision 
    •  median_actual_headway double precision 
    •  segment_bunching_stats 
    •  id uuid primary key default gen_random_uuid() 
    •  segment_id uuid references segments(id) 
    •  route_id text 
    •  direction_id int 
    •  service_id text 
    •  time_of_day_bucket text 
    •  total_headways int 
    •  bunched_headways int 
    •  bunching_rate double precision 
    • Index on  (route_id, time_of_day_bucket) 
    Implement these as materialized views populated from  headways_enriched , or as tables updated via scheduled jobs.
4.  GTFS import and preprocessing
    Implement a Node script (TypeScript, run via  npm  script) to: 1. Download current CTA GTFS zip from CTA GTFS URL. 2. Parse the CSV files ( routes.txt ,  stops.txt ,  trips.txt ,  stop*times.txt ,  shapes.txt ,  calendar\*.txt ) using a streaming parser. 3. Upsert into the  gtfs*\*  tables. 4. After import, compute:
    • For each  (route_id, direction_id) :
    • Create a canonical ordered stop list using a representative trip (e.g., most frequent or longest).
    • Build  route_stop_sequences :
    • Compute cumulative distance along the associated  shape_id  using PostGIS:
    • Construct  LINESTRING  for each  shape_id  from  gtfs_shapes .
    • Project each stop onto the  LINESTRING  and compute distance from the start.
    • Derive segments between adjacent stops and populate  segments  with line geometries from shape. 5. Compute  scheduled_headways :
    • Define time bins (e.g., 15‑minute bins across 24h).
    • For each  (route_id, direction_id, stop_id, service_id, time_bin) :
    • Collect all  departure_time  from  gtfs_stop_times  for trips active under that  service_id  in the time bin.
    • Compute average headway in minutes and insert into  scheduled_headways .
    Write the GTFS importer and preprocessors as idempotent scripts; they should be safe to run on a schedule (e.g., daily/weekly as CTA updates GTFS).
5.  Bus Tracker API integration (Node worker)
    Implement a Node worker service (TypeScript) responsible for:
    4.1. Configuration
    • Read CTA Bus Tracker API key and route list from environment variables.
    • Comply with CTA’s Bus Tracker terms of use (no redistribution of raw data, but allowed to create derivative analyses).
    4.2. Reference data sync
    Build functions to:
    • Call  getroutes  and sync  bt_routes .
    • For each route:
    • Call  getdirections ,  getstops ,  getpatterns  and populate:
    •  bt_stops  with  geom  as a POINT
    •  bt_patterns  with LINESTRING geometries
    •  bt_pattern_stops  with stop sequences
    • Build or refresh  route_map  and  stop_map :
    • For routes: match  bt_routes.rt  to  gtfs_routes.route_short_name  case‑insensitively.
    • For stops: if CTA uses matching IDs, map directly; otherwise spatially join  bt_stops.geom  to nearest  gtfs_stops.geom  within a small radius.
    4.3. Vehicle polling
    Implement a polling loop:
    • Poll  getvehicles  at a configurable interval (start with 30–60s), passing multiple routes per call to stay within limits.
    • For each returned vehicle:
    • Extract:  vid ,  rt ,  des ,  pid ,  lat ,  lon ,  pdist  (as  pdist_feet ),  tmstmp ,  tatripid ,  tablockid .
    • Insert into  bus_positions :
    • Convert  lat ,  lon  to  geom  POINT.
    • Parse  tmstmp  into  timestamptz .
    Make the poller robust:
    • Use exponential backoff on API failures.
    • Handle CTA system downtime gracefully.
    • Ensure inserts are batched and efficient.
6.  Arrival and headway computation logic
    Implement a Node service (could be same worker or a separate job) that periodically processes recent  bus_positions  into  stop_arrivals  and  headways .
    5.1. Arrival detection
    Goal: from polled positions, infer when each bus passes each stop.
    Approach (use  pdist  and known stop distances): 1. Precompute for each  (rt, pid)  a mapping from Bus Tracker pattern to ordered stop list and approximate cumulative distances:
    • Use  bt_pattern_stops  +  bt_stops  and pattern geometry.
    • For each stop on a pattern, compute its distance along  bt_patterns.geom  line. 2. Maintain a per‑bus state machine in memory (or persisted if needed):

    ````
    type BusState = {
    vid: string;
    rt: string;
    pid: string;
    lastPdist: number;
    lastTimestamp: Date;
    lastStopIndex: number; // index in pattern stop list
    };

        ```
            3.	For each new  bus_positions  row (process in order of  tmstmp ):
            •	Look up the bus’s pattern stop list with distances.
            •	Determine the current distance along the pattern ( pdist_feet ).
            •	If  pdist_feet  has passed the distance associated with the next stop in sequence and the bus was previously before that stop:
            •	Record an arrival in  stop_arrivals  with:
            •	 route_id  from  route_map 
            •	 direction_id  (derive from pattern direction or route direction)
            •	 stop_id  from  stop_map 
            •	 vid ,  rt ,  pid 
            •	 arrival_time  = current  tmstmp 
            •	 pdist_feet  = current pdist
            •	Update  lastStopIndex  for that bus.
            4.	Use a small hysteresis if using spatial proximity instead of  pdist  (enter/exit radius around stop).

        Make this job incremental:
        • Process only new  bus_positions  since the last processed timestamp.
        • Store a “watermark” timestamp somewhere (e.g., in a  job_state  table) to pick up where it left off.
        5.2. Headway computation
        For each  (route_id, direction_id, stop_id) : 1. Periodically (e.g., every 5–10 minutes) query recent  stop_arrivals  and compute headways:
        • For each stop, sort arrivals by  arrival_time .
        • For each consecutive pair  (prev, curr) :
        • Compute  headway_min = (curr.arrival_time - prev.arrival_time) / 60000 . 2. Insert a row into  headways :
        •  route_id ,  direction_id ,  stop_id 
        •  prev_vid ,  curr_vid 
        •  arrival_time  (curr)
        •  headway_min 
        • Determine  segment_id  as the segment whose  to_stop_id  is  stop_id .
        Store only new headways (dedupe by  (route_id, direction_id, stop_id, arrival_time, curr_vid) ).

    ````

7.  Enrich headways with schedule and classify bunching
    Implement a job (cron or Supabase scheduled function) that: 1. For each new  headways  row, derive:
    •  service*id  based on  arrival_time::date  and  gtfs_calendar  /  gtfs_calendar_dates .
    •  time_bin_start  based on  arrival_time  truncated to your bin size (e.g., 15 minutes). 2. Lookup scheduled headway from  scheduled_headways  using:
    •  (route_id, direction_id, stop_id, service_id, time_bin_start) .
    • If exact match not found, allow nearest time bin within a reasonable tolerance. 3. Compute:
    •  actual_headway_min = headway_min 
    •  scheduled_headway_min = sched_hw 
    •  hw_ratio = actual_headway_min / scheduled_headway_min  4. Classify:
    •  bunched = (actual_headway_min < 0.25 * scheduled*headway_min)  (relative threshold based on bus performance research).
    •  super_bunched = (actual_headway_min <= 1.0)  (<= 60s gap, similar to CTA’s 1‑minute threshold).
    •  gapped = (actual_headway_min > 1.75 * scheduled_headway_min)  (optional). 5. Insert into  headways_enriched .
8.  Aggregation jobs
    Implement jobs or materialized views to keep the stats tables up to date (e.g., daily or hourly refresh):
    7.1. Route‑level stats
    For a chosen time window (e.g., last 30 days):
    • Group  headways_enriched  by:
    •  route_id ,  direction_id ,  service_id ,  time_of_day_bucket 
    • Define  time_of_day_bucket  function:
    • Example buckets:  "AM_peak"  (07:00–09:59),  "Midday"  (10:00–14:59),  "PM_peak"  (15:00–18:59),  "Evening" ,  "Night" .
    • Compute:
    •  total_headways 
    •  bunched_headways  (count where  bunched = true )
    •  super_bunched_headways 
    •  bunching_rate = bunched_headways::float / total_headways 
    •  avg_hw_ratio 
    •  median_actual_headway  (use Postgres  percentile_cont ).
    Store or refresh  route_bunching_stats .
    7.2. Segment‑level stats
    Similarly aggregate  headways_enriched  by:
    •  segment_id ,  route_id ,  direction_id ,  service_id ,  time_of_day_bucket 
    • Compute same metrics and populate  segment_bunching_stats .
9.  API design (SvelteKit server routes)
    Assume SvelteKit with file‑based routing. Implement REST or JSON endpoints under  /api .
    Implement at least: 1.  GET /api/routes 
    • Returns all routes with summary stats (join  gtfs_routes  with  route_bunching_stats  aggregated overall).
    • Query params:
    •  service_id  (optional)
    •  time_of_day_bucket  (optional) 2.  GET /api/routes/routeId/stats 
    • Returns detailed stats for a single route:
    • Bunching by  time_of_day_bucket 
    • Overall metrics 3.  GET /api/routes/routeId/segments 
    • Returns segments for a route with geometry and stats:
    • Join  segments  with  segment_bunching_stats .
    • Response should be GeoJSON FeatureCollection:
    •  geometry  = segment line
    •  properties  include  segment_id ,  bunching_rate ,  total_headways ,  time_of_day_bucket  (filter by query param). 4.  GET /api/routes/routeId/headways 
    • Paginated list or aggregated headways at stop level for debugging:
    • Query params:  date ,  stop_id ,  limit .
    Implement request validation and error handling. Use Supabase client or direct Postgres connection via connection string.
10. Frontend (Svelte / SvelteKit) UI
    Use SvelteKit + a map library, e.g.  svelte-mapbox  or MapLibre with Mapbox tiles.
    9.1. Layout and pages
    Implement pages: 1.  /  – Overview
    • Show a table of routes sorted by worst bunching rate.
    • Filters:
    • Service type (weekday/weekend/holiday)
    • Time of day bucket
    • Columns:
    • Route short name, route long name, bunching rate, total headways, avg headway ratio.
    • Clicking a route navigates to  /route/routeId . 2.  /route/routeId  – Route detail
    • Header with route name and basic stats.
    • Components:
    • Chart (basic implementation with any lightweight chart lib or custom SVG):
    • Bunching rate vs time of day.
    • Map:
    • Centered on route.
    • Fetch  /api/routes/routeId/segments  and render segments as polylines.
    • Color scale by  bunching_rate  (e.g., green–yellow–red).
    • Map controls to select:
    • Time of day bucket
    • Service type
    • Segment list:
    • Table listing worst segments:
    • From stop name, to stop name, bunching rate, total headways. 3.  /debug  (optional)
    • Visualizations for headways at a selected stop to debug the pipeline.
    9.2. Svelte components
    Implement reusable components:
    •  RouteTable.svelte  – accepts an array of route stats and emits  selectRoute .
    •  RouteStatsSummary.svelte  – displays summary metrics for a route.
    •  BunchingChart.svelte  – simple bar chart of bunching rate by time bucket.
    •  RouteMap.svelte  – wraps Mapbox / MapLibre map:
    • Props:  segmentsGeoJson ,  selectedTimeBucket .
    • Renders polylines with color by bunching rate.
    • Tooltips for segment stats on hover/click.
    Use SvelteKit load functions to fetch data on the server side where appropriate for SEO and performance.
11. DevOps and misc
    • Use environment variables for:
    • Supabase URL / keys
    • CTA Bus Tracker API key
    • Mapbox access token (if using Mapbox).
    • Provide  README  with:
    • How to run migrations with Supabase.
    • How to run the GTFS importer.
    • How to start the Bus Tracker poller.
    • How to start SvelteKit dev server.
    • Consider adding basic observability:
    • Simple logs for poller and jobs.
    • Tables or logs for job run history and last processed timestamps.
12. Quality expectations
    • Use TypeScript for Node and SvelteKit.
    • Organize code cleanly:
    •  src/lib/server/db.ts  – database access helpers.
    •  src/lib/server/gtfsImporter.ts 
    •  src/lib/server/busTrackerPoller.ts 
    •  src/lib/server/headwayProcessor.ts 
    • Include unit tests for:
    • Headway computation from synthetic  stop_arrivals .
    • Bunching classification given synthetic schedules and headways.
    • Make the entire system runnable locally with  docker-compose  or Supabase CLI +  npm run dev .
    Build the project end‑to‑end according to this specification.
