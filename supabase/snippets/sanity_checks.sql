with params as (
  select
    greatest($1::integer, 1) as window_days,
    greatest($2::integer, 1) as stats_window_days,
    greatest($3::double precision, 0.0) as all_segments_bunched_min_rate,
    greatest($4::double precision, 0.0) as all_segments_groups_warn_threshold,
    greatest($5::double precision, 0.0) as null_scheduled_warn_threshold,
    greatest($6::double precision, 0.0) as bunched_warn_threshold,
    greatest($7::double precision, 0.0) as rate_tolerance
),
window_headways as (
  select h.*
  from headways h
  cross join params p
  where h.arrival_time >= now() - make_interval(days => p.window_days)
),
window_enriched as (
  select he.*
  from headways_enriched he
  cross join params p
  where he.arrival_time >= now() - make_interval(days => p.window_days)
),
stats_window_enriched as (
  select he.*
  from headways_enriched he
  cross join params p
  where he.arrival_time >= now() - make_interval(days => p.stats_window_days)
),
missing_headway_id as (
  select
    count(*)::bigint as missing_count,
    count(*) filter (where headway_id is null)::bigint as missing_headway_id_count
  from window_enriched
),
orphan_headway_ref as (
  select count(*)::bigint as orphan_count
  from window_enriched he
  left join headways h on h.id = he.headway_id
  where he.headway_id is not null
    and h.id is null
),
negative_headways as (
  select
    count(*)::bigint as negative_count,
    min(headway_min) as min_headway_min
  from window_headways
  where headway_min < 0
),
recomputed_route_stats as (
  select
    he.route_id,
    he.direction_id,
    he.service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time)) as time_of_day_bucket,
    count(*)::integer as total_headways,
    count(*) filter (where he.bunched)::integer as bunched_headways,
    count(*) filter (where he.super_bunched)::integer as super_bunched_headways,
    avg((he.bunched)::int)::double precision as bunching_rate,
    avg(he.hw_ratio)::double precision as avg_hw_ratio,
    percentile_cont(0.5) within group (order by he.actual_headway_min) as median_actual_headway
  from stats_window_enriched he
  group by
    he.route_id,
    he.direction_id,
    he.service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time))
),
route_stats_diff as (
  select
    coalesce(rs.route_id, er.route_id) as route_id,
    coalesce(rs.direction_id, er.direction_id) as direction_id,
    coalesce(rs.service_id, er.service_id) as service_id,
    coalesce(rs.time_of_day_bucket, er.time_of_day_bucket) as time_of_day_bucket,
    rs.total_headways as expected_total_headways,
    er.total_headways as actual_total_headways,
    rs.bunched_headways as expected_bunched_headways,
    er.bunched_headways as actual_bunched_headways,
    rs.super_bunched_headways as expected_super_bunched_headways,
    er.super_bunched_headways as actual_super_bunched_headways,
    rs.bunching_rate as expected_bunching_rate,
    er.bunching_rate as actual_bunching_rate,
    rs.avg_hw_ratio as expected_avg_hw_ratio,
    er.avg_hw_ratio as actual_avg_hw_ratio,
    rs.median_actual_headway as expected_median_actual_headway,
    er.median_actual_headway as actual_median_actual_headway,
    case
      when rs.route_id is null or er.route_id is null then true
      when rs.total_headways <> er.total_headways then true
      when rs.bunched_headways <> er.bunched_headways then true
      when rs.super_bunched_headways <> er.super_bunched_headways then true
      when abs(coalesce(rs.bunching_rate, 0) - coalesce(er.bunching_rate, 0)) > (select rate_tolerance from params) then true
      when abs(coalesce(rs.avg_hw_ratio, 0) - coalesce(er.avg_hw_ratio, 0)) > (select rate_tolerance from params) then true
      when abs(coalesce(rs.median_actual_headway, 0) - coalesce(er.median_actual_headway, 0)) > (select rate_tolerance from params) then true
      else false
    end as has_mismatch
  from (
    select
      rs.*,
      concat_ws(
        '|',
        coalesce(rs.route_id, '<null>'),
        coalesce(rs.direction_id::text, '<null>'),
        coalesce(rs.service_id, '<null>'),
        coalesce(rs.time_of_day_bucket, '<null>')
      ) as join_key
    from recomputed_route_stats rs
  ) rs
  full join (
    select
      er.*,
      concat_ws(
        '|',
        coalesce(er.route_id, '<null>'),
        coalesce(er.direction_id::text, '<null>'),
        coalesce(er.service_id, '<null>'),
        coalesce(er.time_of_day_bucket, '<null>')
      ) as join_key
    from route_bunching_stats er
  ) er
    on er.join_key = rs.join_key
),
route_stats_summary as (
  select
    count(*) filter (where has_mismatch)::bigint as mismatch_count,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'route_id', route_id,
          'direction_id', direction_id,
          'service_id', service_id,
          'time_of_day_bucket', time_of_day_bucket,
          'expected_total_headways', expected_total_headways,
          'actual_total_headways', actual_total_headways,
          'expected_bunched_headways', expected_bunched_headways,
          'actual_bunched_headways', actual_bunched_headways,
          'expected_super_bunched_headways', expected_super_bunched_headways,
          'actual_super_bunched_headways', actual_super_bunched_headways,
          'expected_bunching_rate', expected_bunching_rate,
          'actual_bunching_rate', actual_bunching_rate
        )
      ) filter (where has_mismatch),
      '[]'::jsonb
    ) as mismatch_examples
  from (
    select *
    from route_stats_diff
    where has_mismatch
    order by route_id nulls last, direction_id nulls last, service_id nulls last, time_of_day_bucket nulls last
    limit 20
  ) sampled
),
recomputed_segment_stats as (
  select
    he.segment_id,
    he.route_id,
    he.direction_id,
    he.service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time)) as time_of_day_bucket,
    count(*)::integer as total_headways,
    count(*) filter (where he.bunched)::integer as bunched_headways,
    avg((he.bunched)::int)::double precision as bunching_rate
  from stats_window_enriched he
  where he.segment_id is not null
  group by
    he.segment_id,
    he.route_id,
    he.direction_id,
    he.service_id,
    coalesce(he.time_of_day_bucket, time_of_day_bucket(he.arrival_time))
),
segment_stats_diff as (
  select
    coalesce(rs.segment_id, es.segment_id) as segment_id,
    coalesce(rs.route_id, es.route_id) as route_id,
    coalesce(rs.direction_id, es.direction_id) as direction_id,
    coalesce(rs.service_id, es.service_id) as service_id,
    coalesce(rs.time_of_day_bucket, es.time_of_day_bucket) as time_of_day_bucket,
    rs.total_headways as expected_total_headways,
    es.total_headways as actual_total_headways,
    rs.bunched_headways as expected_bunched_headways,
    es.bunched_headways as actual_bunched_headways,
    rs.bunching_rate as expected_bunching_rate,
    es.bunching_rate as actual_bunching_rate,
    case
      when rs.segment_id is null or es.segment_id is null then true
      when rs.total_headways <> es.total_headways then true
      when rs.bunched_headways <> es.bunched_headways then true
      when abs(coalesce(rs.bunching_rate, 0) - coalesce(es.bunching_rate, 0)) > (select rate_tolerance from params) then true
      else false
    end as has_mismatch
  from (
    select
      rs.*,
      concat_ws(
        '|',
        coalesce(rs.segment_id::text, '<null>'),
        coalesce(rs.route_id, '<null>'),
        coalesce(rs.direction_id::text, '<null>'),
        coalesce(rs.service_id, '<null>'),
        coalesce(rs.time_of_day_bucket, '<null>')
      ) as join_key
    from recomputed_segment_stats rs
  ) rs
  full join (
    select
      es.*,
      concat_ws(
        '|',
        coalesce(es.segment_id::text, '<null>'),
        coalesce(es.route_id, '<null>'),
        coalesce(es.direction_id::text, '<null>'),
        coalesce(es.service_id, '<null>'),
        coalesce(es.time_of_day_bucket, '<null>')
      ) as join_key
    from segment_bunching_stats es
  ) es
    on es.join_key = rs.join_key
),
segment_stats_summary as (
  select
    count(*) filter (where has_mismatch)::bigint as mismatch_count,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'segment_id', segment_id,
          'route_id', route_id,
          'direction_id', direction_id,
          'service_id', service_id,
          'time_of_day_bucket', time_of_day_bucket,
          'expected_total_headways', expected_total_headways,
          'actual_total_headways', actual_total_headways,
          'expected_bunched_headways', expected_bunched_headways,
          'actual_bunched_headways', actual_bunched_headways,
          'expected_bunching_rate', expected_bunching_rate,
          'actual_bunching_rate', actual_bunching_rate
        )
      ) filter (where has_mismatch),
      '[]'::jsonb
    ) as mismatch_examples
  from (
    select *
    from segment_stats_diff
    where has_mismatch
    order by route_id nulls last, direction_id nulls last, service_id nulls last, segment_id nulls last
    limit 20
  ) sampled
),
window_segment_rates as (
  select
    he.route_id,
    he.direction_id,
    he.segment_id,
    count(*)::integer as total_headways,
    avg((he.bunched)::int)::double precision as bunching_rate
  from window_enriched he
  where he.segment_id is not null
  group by he.route_id, he.direction_id, he.segment_id
),
route_direction_segment_summary as (
  select
    route_id,
    direction_id,
    count(*)::integer as segment_count,
    bool_and(bunching_rate >= (select all_segments_bunched_min_rate from params)) as all_segments_above_threshold,
    sum(total_headways)::integer as total_headways
  from window_segment_rates
  group by route_id, direction_id
),
all_segments_bunched_warn as (
  select
    count(*)::integer as route_direction_count,
    count(*) filter (where all_segments_above_threshold)::integer as all_segments_bunched_count,
    coalesce(
      avg(case when all_segments_above_threshold then 1.0 else 0.0 end),
      0.0
    )::double precision as all_segments_bunched_share,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'route_id', route_id,
          'direction_id', direction_id,
          'segment_count', segment_count,
          'total_headways', total_headways
        )
      ) filter (where all_segments_above_threshold),
      '[]'::jsonb
    ) as flagged_groups
  from (
    select *
    from route_direction_segment_summary
    order by route_id, direction_id
    limit 50
  ) sampled
),
null_scheduled_warn as (
  select
    count(*)::bigint as total_rows,
    count(*) filter (where scheduled_headway_min is null)::bigint as null_scheduled_rows,
    coalesce(avg(case when scheduled_headway_min is null then 1.0 else 0.0 end), 0.0)::double precision as null_scheduled_share
  from window_enriched
),
bunched_share_warn as (
  select
    count(*)::bigint as total_rows,
    count(*) filter (where bunched)::bigint as bunched_rows,
    coalesce(avg((bunched)::int), 0.0)::double precision as bunched_share
  from window_enriched
)
select
  'headways_enriched_missing_headway_id'::text as check_name,
  'fail'::text as severity,
  (m.missing_headway_id_count = 0) as passed,
  m.missing_headway_id_count::double precision as metric_value,
  0.0::double precision as threshold,
  jsonb_build_object(
    'window_days', (select window_days from params),
    'window_rows', m.missing_count,
    'missing_headway_id_rows', m.missing_headway_id_count
  ) as details_json
from missing_headway_id m

union all

select
  'headway_id_orphan_references'::text as check_name,
  'fail'::text as severity,
  (o.orphan_count = 0) as passed,
  o.orphan_count::double precision as metric_value,
  0.0::double precision as threshold,
  jsonb_build_object(
    'window_days', (select window_days from params),
    'orphan_rows', o.orphan_count
  ) as details_json
from orphan_headway_ref o

union all

select
  'negative_headways'::text as check_name,
  'fail'::text as severity,
  (n.negative_count = 0) as passed,
  n.negative_count::double precision as metric_value,
  0.0::double precision as threshold,
  jsonb_build_object(
    'window_days', (select window_days from params),
    'negative_rows', n.negative_count,
    'min_headway_min', n.min_headway_min
  ) as details_json
from negative_headways n

union all

select
  'route_bunching_stats_mismatch'::text as check_name,
  'fail'::text as severity,
  (r.mismatch_count = 0) as passed,
  r.mismatch_count::double precision as metric_value,
  0.0::double precision as threshold,
  jsonb_build_object(
    'stats_window_days', (select stats_window_days from params),
    'mismatch_rows', r.mismatch_count,
    'mismatch_examples', r.mismatch_examples
  ) as details_json
from route_stats_summary r

union all

select
  'segment_bunching_stats_mismatch'::text as check_name,
  'fail'::text as severity,
  (s.mismatch_count = 0) as passed,
  s.mismatch_count::double precision as metric_value,
  0.0::double precision as threshold,
  jsonb_build_object(
    'stats_window_days', (select stats_window_days from params),
    'mismatch_rows', s.mismatch_count,
    'mismatch_examples', s.mismatch_examples
  ) as details_json
from segment_stats_summary s

union all

select
  'all_segments_highly_bunched_share'::text as check_name,
  'warn'::text as severity,
  (a.all_segments_bunched_share <= (select all_segments_groups_warn_threshold from params)) as passed,
  a.all_segments_bunched_share as metric_value,
  (select all_segments_groups_warn_threshold from params) as threshold,
  jsonb_build_object(
    'window_days', (select window_days from params),
    'all_segments_bunched_min_rate', (select all_segments_bunched_min_rate from params),
    'route_direction_count', a.route_direction_count,
    'flagged_route_direction_count', a.all_segments_bunched_count,
    'flagged_groups', a.flagged_groups
  ) as details_json
from all_segments_bunched_warn a

union all

select
  'null_scheduled_headway_share'::text as check_name,
  'warn'::text as severity,
  (n.null_scheduled_share <= (select null_scheduled_warn_threshold from params)) as passed,
  n.null_scheduled_share as metric_value,
  (select null_scheduled_warn_threshold from params) as threshold,
  jsonb_build_object(
    'window_days', (select window_days from params),
    'window_rows', n.total_rows,
    'null_scheduled_rows', n.null_scheduled_rows
  ) as details_json
from null_scheduled_warn n

union all

select
  'bunched_rows_share'::text as check_name,
  'warn'::text as severity,
  (b.bunched_share <= (select bunched_warn_threshold from params)) as passed,
  b.bunched_share as metric_value,
  (select bunched_warn_threshold from params) as threshold,
  jsonb_build_object(
    'window_days', (select window_days from params),
    'window_rows', b.total_rows,
    'bunched_rows', b.bunched_rows
  ) as details_json
from bunched_share_warn b

order by severity, check_name;
