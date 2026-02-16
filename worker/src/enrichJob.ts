import { fileURLToPath } from 'node:url';
import { query } from './db';

export const runEnrich = async () => {
  await query('select enrich_headways()');
  await query('select refresh_route_bunching_stats(30)');
  await query('select refresh_segment_bunching_stats(30)');
  console.log('Enrichment and stats refresh complete');
};

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  runEnrich().catch((error) => {
    console.error('Enrichment job failed', error);
    process.exit(1);
  });
}
