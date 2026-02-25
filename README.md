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

Runs continuously. Configure routes and interval via env vars.

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

## Testing

Unit tests for headway computation and bunching classification:

```bash
npm run test
```

## Notes

- `enrich_headways()`, `refresh_route_bunching_stats()` and `refresh_segment_bunching_stats()` are defined in migrations.
- `job_state` tracks incremental watermarks for arrivals and headways processing.
- GTFS import refreshes derived route sequences and segments. If you are running with existing headways, re-run enrichment after import.
