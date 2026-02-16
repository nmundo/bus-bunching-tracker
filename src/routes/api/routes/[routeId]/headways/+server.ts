import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { query } from '$server/db';

export const GET: RequestHandler = async ({ params, url }) => {
  const routeId = params.routeId;
  const date = url.searchParams.get('date');
  const stopId = url.searchParams.get('stop_id');
  const limit = Math.min(Number(url.searchParams.get('limit') ?? 100), 1000);

  const filters: string[] = ['route_id = $1'];
  const paramsList: unknown[] = [routeId];

  if (date) {
    paramsList.push(date);
    filters.push(`arrival_time::date = $${paramsList.length}`);
  }
  if (stopId) {
    paramsList.push(stopId);
    filters.push(`stop_id = $${paramsList.length}`);
  }

  paramsList.push(limit);

  const sql = `
    SELECT
      route_id,
      direction_id,
      stop_id,
      arrival_time,
      actual_headway_min,
      scheduled_headway_min,
      hw_ratio,
      bunched,
      super_bunched,
      gapped
    FROM headways_enriched
    WHERE ${filters.join(' AND ')}
    ORDER BY arrival_time DESC
    LIMIT $${paramsList.length}
  `;

  const result = await query(sql, paramsList);
  return json(result.rows);
};
