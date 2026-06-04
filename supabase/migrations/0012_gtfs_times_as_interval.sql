-- Migrate GTFS time columns from `time` to `interval` so that overnight trip
-- times (e.g. 25:30:00 = 01:30 AM the following service day) are stored with
-- their original GTFS values rather than being wrapped with % 24.
--
-- Background: GTFS deliberately uses times past 24:00 for trips that begin on
-- one calendar day but run into the next. Postgres `time` columns reject these
-- values. The previous workaround was to wrap hours silently, which
-- misclassifies some overnight trips into early-morning time bins. Storing as
-- `interval` preserves the raw value and allows correct comparisons.
--
-- Downstream effects:
--   • computeScheduledHeadways (gtfsImporter.ts) filters with
--       departure_time >= time_bin_start
--     which becomes an interval >= interval comparison — still valid.
--   • scheduled_headways.time_bin_start / time_bin_end stay as `time` because
--     they are derived by bucketing and are always in [00:00, 24:00).
--   • The `bins` CTE in computeScheduledHeadways casts to `::time` and the
--     join condition must be updated to compare intervals to intervals:
--       d.departure_time >= b.time_bin_start::interval
--       and d.departure_time < (b.time_bin_start + interval '15 minutes')
--     This is handled in the updated gtfsImporter.ts computeScheduledHeadways.
--
-- Run this migration BEFORE the next GTFS import so the import does not need
-- to wrap hours any more. After applying, remove the % 24 wrapping from
-- normalizeTime in gtfsImporter.ts (the warning branch) and pass raw values.

-- gtfs_stop_times: arrival_time and departure_time
alter table gtfs_stop_times
  alter column arrival_time type interval using arrival_time::interval,
  alter column departure_time type interval using departure_time::interval;

-- gtfs_frequencies: start_time and end_time
alter table gtfs_frequencies
  alter column start_time type interval using start_time::interval,
  alter column end_time type interval using end_time::interval;

-- Unique index on gtfs_frequencies references (trip_id, start_time, end_time).
-- The index definition does not change; Postgres rebuilds it automatically
-- when the column types change via ALTER TABLE.

-- Verify the cast succeeded and no nulls were introduced unintentionally.
do $$
begin
  if exists (
    select 1 from gtfs_stop_times
    where arrival_time is null or departure_time is null
  ) then
    raise notice 'gtfs_stop_times has null time values after migration — review source data';
  end if;
end;
$$;
