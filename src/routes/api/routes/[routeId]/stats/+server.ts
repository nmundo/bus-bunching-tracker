import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { query } from '$server/db';

export const GET: RequestHandler = async ({ params, url }) => {
  const routeId = params.routeId;
  const serviceId = url.searchParams.get('service_id');

  const baseParams: unknown[] = [routeId];
  const serviceFilter = serviceId ? `AND service_id = $2` : '';
  if (serviceId) baseParams.push(serviceId);

  const routeResult = await query(
    `SELECT route_id, route_short_name, route_long_name FROM gtfs_routes WHERE route_id = $1`,
    [routeId]
  );

  const summarySql = `
    SELECT
      SUM(total_headways)::int AS total_headways,
      SUM(bunched_headways)::int AS bunched_headways,
      CASE
        WHEN SUM(total_headways) > 0
        THEN SUM(bunched_headways)::float / SUM(total_headways)
        ELSE NULL
      END AS bunching_rate,
      AVG(avg_hw_ratio) AS avg_hw_ratio,
      AVG(median_actual_headway) AS median_actual_headway
    FROM route_bunching_stats
    WHERE route_id = $1
    ${serviceFilter}
  `;

  const bucketsSql = `
    SELECT time_of_day_bucket, AVG(bunching_rate) AS bunching_rate
    FROM route_bunching_stats
    WHERE route_id = $1
    ${serviceFilter}
    GROUP BY time_of_day_bucket
    ORDER BY time_of_day_bucket
  `;

  const [summaryResult, bucketsResult] = await Promise.all([
    query(summarySql, baseParams),
    query(bucketsSql, baseParams)
  ]);

  return json({
    route: routeResult.rows[0] ?? null,
    summary: summaryResult.rows[0] ?? null,
    buckets: bucketsResult.rows
  });
};
