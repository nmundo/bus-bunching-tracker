# Bus Bunching Tracker

Production-ready pipeline for analyzing CTA bus bunching with Supabase + PostGIS, a Node worker, and a SvelteKit UI.

## Stack

- Supabase Postgres + PostGIS
- Node.js TypeScript worker for data ingestion and processing
- SvelteKit for server-rendered pages and server functions
- MapLibre for map rendering

## Setup

1. Install dependencies:

```bash
pnpm install
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
pnpm run gtfs:import
```

CTA GTFS zip:

- https://www.transitchicago.com/downloads/sch_data/google_transit.zip

### Bus Tracker reference sync

Syncs routes, stops, and patterns from the CTA Bus Tracker API.

```bash
pnpm run sync:run
```

CTA Bus Tracker API base:

- https://www.ctabustracker.com/bustime/api/v3

### Vehicle polling

Runs continuously with dynamic route-tiering:
- `getvehicles` requests are batched with a hard cap of 10 routes per call.
- Fast tier routes (recently active) are polled every cycle.
- Slow tier routes are bucketed and rotated to limit call volume while preserving network coverage.
- Routes that return no vehicles are not treated as errors — they are skipped silently.

Key env vars:
- `CTA_BUS_TRACKER_POLL_INTERVAL_SEC` (default `45`)
- `CTA_BUS_TRACKER_BATCH_SIZE` (default `6`, capped at `10`)
- `CTA_BUS_TRACKER_LOW_TIER_MAX_STALENESS_SEC` (default `360`)
- `CTA_BUS_TRACKER_ACTIVITY_TTL_SEC` (default `360`)
- `CTA_BUS_TRACKER_ROUTE_REFRESH_SEC` (default `900`)
- Optional debug override: `CTA_BUS_TRACKER_ROUTES=1,2,3`

```bash
pnpm run poller:run
```

### Arrivals + headways

Batch processors to turn raw positions into arrivals and headways.

```bash
pnpm run arrivals:run
pnpm run headways:run
```

### Enrichment + stats

Calls SQL functions to enrich headways and refresh aggregates.

```bash
pnpm run enrich:run
```

### Daily snapshot

Captures one local (Chicago) calendar day of enriched headways into `route_daily_bunching_stats`. Defaults to yesterday; pass a `YYYY-MM-DD` argument to backfill a specific day.

```bash
pnpm run tsx -- worker/src/dailySnapshotJob.ts [YYYY-MM-DD]
```

### Serving publish

Copies aggregated stats from the warehouse DB to the serving DB and updates `publish_meta`. Runs enrichment first, then publishes all reference and stats tables atomically.

Key env vars:
- `SERVING_DATABASE_URL` — connection string for the serving DB (required)
- `SERVING_WINDOW_DAYS` (default `30`)

```bash
pnpm run publish:run
```

## Worker runtime

A single long-running worker that drives the continuous poller and schedules all cron jobs:

```bash
pnpm run worker:dev
```

### Cron schedule

| Job | Schedule | Notes |
|-----|----------|-------|
| Reference sync | 02:30 daily | Syncs CTA Bus Tracker routes, stops, patterns |
| Arrivals | Every 5 min | Incremental; picks up from last watermark |
| Headways | Every 10 min | Incremental; picks up from last watermark |
| Enrichment | :15 past each hour | Enriches unenriched headways in batches |
| Daily snapshot | 03:15 Chicago | Captures previous calendar day into daily trend table |
| Serving publish | 03:30 Chicago | Publishes warehouse → serving DB |

Each job is guarded against overlapping runs.

## Database architecture

The pipeline uses two separate Postgres databases:

- **Warehouse DB** (`DATABASE_URL`): raw positions, arrivals, headways, enriched headways, and all aggregate stats. This is where the worker writes.
- **Serving DB** (`SERVING_DATABASE_URL`): a read-optimized copy of the aggregate and reference tables, updated by the publish job. The SvelteKit app reads from this database.

The serving DB holds: `gtfs_routes`, `gtfs_stops`, `gtfs_calendar`, `segments`, `route_direction_labels`, `route_bunching_stats`, `segment_bunching_stats`, `route_hourly_bunching_stats`, `route_daily_bunching_stats`, and `publish_meta`.

When running without a separate serving DB (e.g. local development), point both `DATABASE_URL` and `SERVING_DATABASE_URL` to the same database.

## Methodology

### 1. Data collection

1. GTFS static data establishes the planned service baseline (`gtfs_*`, `route_stop_sequences`, `segments`, `scheduled_headways`).
2. CTA Bus Tracker reference data (`routes`, `stops`, `patterns`) is synced and mapped to GTFS via:
   - `route_map` (Bus Tracker route code → GTFS route)
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

A headway is **analyzable** if it has a scheduled baseline and falls within a sanity cap. Only analyzable headways count toward schedule-relative metrics (bunching rate, gapping rate, EWT, CV, mean headways). `total_headways` includes all observations including super-bunched; `analyzable_headways` is the denominator for everything else.

Time dimensions are computed in `America/Chicago`:
- `time_bin_start`: arrival time floored to a 15-minute bin
- `time_of_day_bucket`:
  - `AM_peak`: 07:00-09:59
  - `Midday`: 10:00-14:59
  - `PM_peak`: 15:00-18:59
  - `Evening`: 19:00-22:59
  - `Night`: otherwise

### 4. Aggregate stats and metrics

Stats refresh jobs aggregate the most recent 30 days. All aggregate tables store **sufficient statistics** — additive components (`sum_actual_hw`, `sum_actual_hw_sq`, `sum_sched_hw`, `sum_sched_hw_sq`) alongside counts — so metrics can be pooled exactly across any grouping without losing precision.

Derived metrics (computed on the fly from summed components):

| Metric | Formula |
|--------|---------|
| `bunching_rate` | `bunched_headways / analyzable_headways` |
| `super_bunching_rate` | `super_bunched_headways / total_headways` |
| `gapping_rate` | `gapped_headways / analyzable_headways` |
| `mean_scheduled_headway` | `sum_sched_hw / analyzable_headways` |
| `mean_actual_headway` | `sum_actual_hw / analyzable_headways` |
| `excess_wait_min` | `Σh² / (2·Σh) − ΣH² / (2·ΣH)` (actual vs scheduled random-rider wait) |
| `headway_cv` | `stddev(actual headways) / mean(actual headways)` |

Route-level (`route_bunching_stats`), grouped by
`(route_id, direction_id, service_id, time_of_day_bucket)`.

Hourly profile (`route_hourly_bunching_stats`), grouped by
`(route_id, service_id, hour_of_day)`.

Daily trend (`route_daily_bunching_stats`), grouped by
`(route_id, service_id, stat_date)`. Accumulated by the daily snapshot job; not pruned when records age out of the 30-day enrichment window.

Segment-level (`segment_bunching_stats`), grouped by
`(segment_id, route_id, direction_id, service_id, time_of_day_bucket)`:
- `total_headways`, `bunched_headways`, `bunching_rate`

### 5. UI pages

- `/` — network overview: route table sortable by bunching rate, EWT, headway CV, etc.; network-level hourly bunching profile chart; service and time-of-day filters.
- `/route/[routeId]` — route detail: summary metrics, 24-hour bunching profile, daily trend chart, segment map with per-segment bunching heat.

Filters available on both pages:
- `service_id` — exact service ID, or shorthand `weekday` / `saturday` / `sunday` (resolved from `gtfs_calendar`)
- `time_of_day_bucket` — one of `AM_peak`, `Midday`, `PM_peak`, `Evening`, `Night`
- `direction_id` — numeric direction (route detail page)

Data is fetched via SvelteKit server functions using `$app/server`'s `query()` helper; there are no public REST API routes.

## Vercel deployment

The SvelteKit app can be deployed to Vercel. The long-running ingestion worker cannot, because it relies on cron scheduling plus a continuous poller loop.

### 1. Use the Vercel adapter

This repo is configured for `@sveltejs/adapter-vercel`, so `pnpm run build` will produce a Vercel deployment build.

### 2. Set Vercel environment variables

- `DATABASE_URL` — Supabase pooled serverless connection string (serving DB in production)
- `PUBLIC_SUPABASE_URL` and `PUBLIC_SUPABASE_PUBLISHABLE_KEY` — optional; the browser does not talk to Supabase directly

### 3. Keep the worker on a separate host

Run the worker somewhere that supports long-lived Node processes:

```bash
pnpm run worker:dev
```

Set both `DATABASE_URL` (warehouse) and `SERVING_DATABASE_URL` (serving) in the worker's environment.

## Testing

Unit tests for headway computation and bunching classification:

```bash
pnpm run test
```

Integration sanity tests against a real database (requires `RUN_DB_TESTS=1`):

```bash
pnpm run test:sanity
```

## Notes

- `enrich_headways_batch_safe()`, `refresh_route_bunching_stats()`, `refresh_segment_bunching_stats()`, and `snapshot_daily_bunching_stats()` are defined in migrations.
- `job_state` tracks incremental watermarks for arrivals and headways processing.
- GTFS import refreshes derived route sequences and segments. If you have existing headways, re-run enrichment after import.
- Buses are considered bunched if the actual headway is less than 25% of their scheduled headway, and super-bunched if the actual headway is ≤ 1 minute.
- Excess wait (`excess_wait_min`) is the additional wait time suffered by a random-arrival rider relative to the scheduled baseline: `E[H²]/(2·E[H])` computed from actual vs scheduled headways over the same analyzable observations.
