# Bus Bunching Tracker

Production-ready pipeline for analyzing CTA bus bunching with Supabase + PostGIS, a Node worker, and a SvelteKit UI.

## Stack

- Supabase Postgres + PostGIS
- Node.js TypeScript worker for data ingestion and processing
- SvelteKit for API routes and frontend
- MapLibre for map rendering

## Setup

1. Install dependencies:

```bash
npm install
```

2. Copy `.env.example` to `.env` and update values.

3. Initialize Supabase and run migrations:

```bash
supabase db push
```

## Data Pipeline

### GTFS import

Downloads CTA GTFS and populates `gtfs_*`, `route_stop_sequences`, `segments`, and `scheduled_headways`.
If CTA does not provide `frequencies.txt`, the importer synthesizes `gtfs_frequencies` from scheduled headways.

```bash
npm run gtfs:import
```

CTA GTFS zip:

- https://www.transitchicago.com/downloads/sch_data/google_transit.zip

### Bus Tracker reference sync

Syncs routes, stops, and patterns from the CTA Bus Tracker API.

```bash
npm run sync:run
```

CTA Bus Tracker API base:

- https://www.ctabustracker.com/bustime/api/v3

### Vehicle polling

Runs continuously with dynamic route-tiering:
- `getvehicles` requests are batched with a hard cap of 10 routes per call.
- Fast tier routes (recently active) are polled every cycle.
- Slow tier routes are bucketed and rotated to limit call volume while preserving network coverage.

Key env vars:
- `CTA_BUS_TRACKER_POLL_INTERVAL_SEC` (default `45`)
- `CTA_BUS_TRACKER_BATCH_SIZE` (default `6`, capped at `10`)
- `CTA_BUS_TRACKER_LOW_TIER_MAX_STALENESS_SEC` (default `360`)
- `CTA_BUS_TRACKER_ACTIVITY_TTL_SEC` (default `360`)
- `CTA_BUS_TRACKER_ROUTE_REFRESH_SEC` (default `900`)
- Optional debug override: `CTA_BUS_TRACKER_ROUTES=1,2,3`

```bash
npm run poller:run
```

### Arrivals + headways

Batch processors to turn raw positions into arrivals and headways.

```bash
npm run arrivals:run
npm run headways:run
```

### Enrichment + stats

Calls SQL functions to enrich headways and refresh aggregates.

```bash
npm run enrich:run
```

## Methodology

### 1. Data collection methodology

1. GTFS static data is imported to establish the planned service baseline (`gtfs_*`, `route_stop_sequences`, `segments`, `scheduled_headways`).
2. CTA Bus Tracker reference data (`routes`, `stops`, `patterns`) is synced and mapped to GTFS via:
   - `route_map` (Bus Tracker route code -> GTFS route)
   - `stop_map` (exact stop ID match first, then nearest-stop spatial match)
3. Vehicle positions are polled continuously from CTA Bus Tracker and stored in `bus_positions`.
4. Stop arrivals are inferred from each vehicle's `pdist_feet` progress along its pattern, producing `stop_arrivals`.
5. Actual headways are computed from consecutive arrivals at each `(route_id, direction_id, stop_id)` and stored in `headways`.
6. Headways are enriched with schedule context (service/day/time bin) and bunching flags in `headways_enriched`.

### 2. Scheduled and actual headway calculations

- Scheduled headways are precomputed from GTFS stop departure times in 15-minute bins:
  - Grouping key: `(route_id, direction_id, stop_id, service_id, time_bin_start)`
  - `scheduled_headway_min = avg(time between consecutive scheduled departures in bin)`
- Actual headways come from observed arrivals:
  - Grouping key: `(route_id, direction_id, stop_id)` ordered by `arrival_time`
  - `actual_headway_min = (curr_arrival_time - prev_arrival_time) / 60`

### 3. Enrichment and classification rules

Each enriched record contains:
- `hw_ratio = actual_headway_min / scheduled_headway_min`
- `bunched = actual_headway_min < 0.25 * scheduled_headway_min`
- `super_bunched = actual_headway_min <= 1.0`
- `gapped = actual_headway_min > 1.75 * scheduled_headway_min`

Time dimensions are computed in `America/Chicago`:
- `time_bin_start`: arrival time floored to a 15-minute bin
- `time_of_day_bucket`:
  - `AM_peak`: 07:00-09:59
  - `Midday`: 10:00-14:59
  - `PM_peak`: 15:00-18:59
  - `Evening`: 19:00-22:59
  - `Night`: otherwise

### 4. Aggregate stats and breakdowns

Stats refresh jobs aggregate the most recent 30 days.

Route-level (`route_bunching_stats`), grouped by
`(route_id, direction_id, service_id, time_of_day_bucket)`:
- `total_headways = count(*)`
- `bunched_headways = count(*) where bunched`
- `super_bunched_headways = count(*) where super_bunched`
- `bunching_rate = bunched_headways / total_headways`
- `avg_hw_ratio = avg(hw_ratio)`
- `median_actual_headway = percentile_cont(0.5)`

Segment-level (`segment_bunching_stats`), grouped by
`(segment_id, route_id, direction_id, service_id, time_of_day_bucket)`:
- `total_headways = count(*)`
- `bunched_headways = count(*) where bunched`
- `bunching_rate = bunched_headways / total_headways`

### 5. API/UI breakdown behavior

- `GET /api/routes` returns network table metrics by summing route stats and recomputing `bunching_rate` as `SUM(bunched_headways) / SUM(total_headways)`.
- `GET /api/routes/[routeId]/stats` returns:
  - route summary metrics
  - hourly bunching profile (0-23 local hour) from enriched headways
- `GET /api/routes/[routeId]/segments` returns segment GeoJSON with per-segment bunching metrics.
- Service filtering supports:
  - exact `service_id`
  - `weekday`, `saturday`, `sunday` (derived from `gtfs_calendar`)

## Worker runtime

For a single long-running worker that polls and schedules jobs:

```bash
npm run worker:dev
```

## API routes

- `GET /api/routes`
- `GET /api/routes/[routeId]/stats`
- `GET /api/routes/[routeId]/segments`
- `GET /api/routes/[routeId]/headways`

## Frontend

Start the SvelteKit dev server:

```bash
npm run dev
```

## Vercel deployment

The SvelteKit app can be deployed to Vercel. The long-running ingestion worker cannot, because it relies on cron scheduling plus a continuous poller loop.

### 1. Use the Vercel adapter

This repo is configured for `@sveltejs/adapter-vercel`, so `npm run build` will produce a Vercel deployment build.

### 2. Set Vercel environment variables

For the web app, set these in the Vercel project:

- `DATABASE_URL`
  - Use Supabase's pooled serverless connection string, not a direct database connection string.
- `PUBLIC_SUPABASE_URL` and `PUBLIC_SUPABASE_PUBLISHABLE_KEY`
  - Optional for this repo today, since the browser does not talk to Supabase directly.

### 3. Use server routes for database access

The frontend should continue to call SvelteKit API routes under `src/routes/api/**`. Database access stays on the server through `src/lib/server/db.ts`, so secrets are not bundled into client-side code.

### 4. Keep the worker on a separate host

Run the worker somewhere else that supports long-lived Node processes:

```bash
npm run worker:dev
```

## Testing

Unit tests for headway computation and bunching classification:

```bash
npm run test
```

## Notes

- `enrich_headways()`, `refresh_route_bunching_stats()` and `refresh_segment_bunching_stats()` are defined in migrations.
- `job_state` tracks incremental watermarks for arrivals and headways processing.
- GTFS import refreshes derived route sequences and segments. If you are running with existing headways, re-run enrichment after import.
- Busses are considered bunched if the actual headway between them is less than 25% of their scheduled headway, and super bunched if the actual headway is less than a minute
