-- Metric accuracy: store summed "sufficient statistics" so the non-linear
-- metrics (excess wait, headway CV, mean headways) can be re-aggregated exactly
-- across buckets/services/routes instead of averaging finished per-bucket values.
--
-- Also: excess wait / CV / mean headways and the bunching & gapping rates are now
-- computed over "analyzable" headways only — those with a scheduled baseline and
-- within a 180-minute sanity cap — which removes overnight / service-gap outliers
-- and stops schedule-unmatched headways from diluting the rates.
--
-- After applying these column adds, re-run the function definitions in
-- 0003_functions.sql (refresh_bunching_stats, snapshot_daily_bunching_stats) and
-- then `select refresh_bunching_stats(30)` to repopulate. Apply the matching
-- column adds to the serving DB from supabase/snippets/serving_schema_geojson.sql.

alter table route_bunching_stats add column if not exists analyzable_headways int;
alter table route_bunching_stats add column if not exists sum_actual_hw double precision;
alter table route_bunching_stats add column if not exists sum_actual_hw_sq double precision;
alter table route_bunching_stats add column if not exists sum_sched_hw double precision;
alter table route_bunching_stats add column if not exists sum_sched_hw_sq double precision;

alter table route_daily_bunching_stats add column if not exists analyzable_headways int;
alter table route_daily_bunching_stats add column if not exists sum_actual_hw double precision;
alter table route_daily_bunching_stats add column if not exists sum_actual_hw_sq double precision;
alter table route_daily_bunching_stats add column if not exists sum_sched_hw double precision;
alter table route_daily_bunching_stats add column if not exists sum_sched_hw_sq double precision;
