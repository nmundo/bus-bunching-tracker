import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { query } from '$server/db';

export const GET: RequestHandler = async ({ url }) => {
  const serviceId = url.searchParams.get('service_id');
  const bucket = url.searchParams.get('time_of_day_bucket');

  const filters: string[] = [];
  const params: unknown[] = [];

  if (serviceId) {
    params.push(serviceId);
    filters.push(`rbs.service_id = $${params.length}`);
  }
  if (bucket) {
    params.push(bucket);
    filters.push(`rbs.time_of_day_bucket = $${params.length}`);
  }

  const whereSql = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

  const sql = `
    SELECT
      r.route_id,
      r.route_short_name,
      r.route_long_name,
      SUM(rbs.total_headways)::int AS total_headways,
      SUM(rbs.bunched_headways)::int AS bunched_headways,
      CASE
        WHEN SUM(rbs.total_headways) > 0
        THEN SUM(rbs.bunched_headways)::float / SUM(rbs.total_headways)
        ELSE NULL
      END AS bunching_rate,
      AVG(rbs.avg_hw_ratio) AS avg_hw_ratio
    FROM gtfs_routes r
    LEFT JOIN route_bunching_stats rbs ON rbs.route_id = r.route_id
    ${whereSql}
    GROUP BY r.route_id, r.route_short_name, r.route_long_name
    ORDER BY bunching_rate DESC NULLS LAST, r.route_short_name
  `;

  const result = await query(sql, params);
  return json(result.rows);
};
