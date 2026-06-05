-- Arrivals deduplication and pattern-stop integrity fixes.
--
-- Three problems addressed:
--
-- 1. stop_arrivals had no unique constraint, so ON CONFLICT DO NOTHING in the
--    arrivals processor was a no-op and duplicates accumulated on every run.
--    Fix: add a unique index and clean up exact duplicates first.
--
-- 2. bt_pattern_stops had no unique constraint on (pid, seq), so each nightly
--    sync appended another copy of every pattern stop instead of updating it.
--    Fix: deduplicate and add a unique index.
--
-- 3. The arrivals processor recomputed ST_LineLocatePoint / ST_Length for every
--    pattern stop on every 5-minute run even though the values are stable between
--    syncs.  Fix: store distance_feet on bt_pattern_stops and backfill.

-- ── 1. stop_arrivals deduplication ──────────────────────────────────────────

-- Remove exact duplicates (same route, stop, vehicle, timestamp) that may have
-- been produced by the duplicate-pattern-stop issue below.  Keep the lowest id
-- (earliest insertion) for each group.
--
-- Uses a self-join DELETE rather than NOT IN (SELECT min(id) ...) because the
-- NOT IN form does a nested-loop scan that is O(n²) and unworkable on tables
-- larger than a few hundred thousand rows.
delete from stop_arrivals a
using stop_arrivals b
where a.route_id     = b.route_id
  and a.stop_id      = b.stop_id
  and a.vid          = b.vid
  and a.arrival_time = b.arrival_time
  and a.id           > b.id;

create unique index if not exists stop_arrivals_dedup_idx
  on stop_arrivals (route_id, stop_id, vid, arrival_time);

-- ── 2. bt_pattern_stops deduplication + unique constraint ───────────────────

-- Keep the first row (by id) for each (pid, seq) pair and drop the rest.
-- Self-join form avoids the NOT IN / subquery nested-loop problem at scale.
delete from bt_pattern_stops a
using bt_pattern_stops b
where a.pid = b.pid
  and a.seq = b.seq
  and a.id  > b.id;

create unique index if not exists bt_pattern_stops_pid_seq_idx
  on bt_pattern_stops (pid, seq);

-- ── 3. Precomputed pattern-stop distance ────────────────────────────────────

alter table bt_pattern_stops
  add column if not exists distance_feet double precision;

-- Backfill for existing rows.  Rows where either geom is still null (stop or
-- pattern not yet synced) remain null and are skipped by the arrivals processor
-- until the next nightly sync populates them.
update bt_pattern_stops ps
set distance_feet = subq.distance_feet
from (
  select
    ps2.id,
    ST_LineLocatePoint(bp.geom, bs.geom)
      * ST_Length(bp.geom::geography)
      * 3.28084 as distance_feet
  from bt_pattern_stops ps2
  join bt_patterns bp on bp.pid = ps2.pid
  join bt_stops    bs on bs.stpid = ps2.stpid
  where bp.geom is not null
    and bs.geom is not null
) subq
where ps.id = subq.id;
