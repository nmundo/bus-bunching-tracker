import { fileURLToPath } from 'node:url';
import { busTimeRequest } from './busTrackerClient';
import { query } from './db';
import { optionalEnv } from './env';

const DEFAULT_INTERVAL = 45;
const BATCH_SIZE = Number(optionalEnv('CTA_BUS_TRACKER_BATCH_SIZE', '6'));

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const chunk = <T>(list: T[], size: number) => {
  const result: T[][] = [];
  for (let i = 0; i < list.length; i += size) {
    result.push(list.slice(i, i + size));
  }
  return result;
};

type Vehicle = {
  vid: string;
  rt: string;
  des: string;
  pid?: string;
  lat: string;
  lon: string;
  pdist?: string;
  tmstmp: string;
  tatripid?: string;
  tablockid?: string;
};

type VehiclesResponse = { vehicle?: Vehicle[]; vehicles?: Vehicle[] };

const parseTimestamp = (value: string) => value;

const loadKnownPatterns = async () => {
  const result = await query<{ pid: string }>('select pid from bt_patterns');
  return new Set(result.rows.map((row) => row.pid));
};

const insertVehicles = async (vehicles: Vehicle[]) => {
  if (!vehicles.length) return;

  const knownPatterns = await loadKnownPatterns();
  const values = vehicles.map((v) => [
    v.vid,
    v.rt,
    v.des,
    v.pid && knownPatterns.has(v.pid) ? v.pid : null,
    Number(v.lat),
    Number(v.lon),
    v.pdist ? Number(v.pdist) : null,
    parseTimestamp(v.tmstmp),
    v.tatripid ?? null,
    v.tablockid ?? null
  ]);

  const placeholders = values
    .map((row, index) => {
      const offset = index * 10;
      return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, ST_SetSRID(ST_MakePoint($${offset + 6}, $${offset + 5}), 4326), $${offset + 7}, (to_timestamp($${offset + 8}, 'YYYYMMDD HH24:MI') at time zone 'America/Chicago'), $${offset + 9}, $${offset + 10})`;
    })
    .join(', ');

  const sql = `
    insert into bus_positions (
      vid, rt, des, pid, lat, lon, geom, pdist_feet, tmstmp, tatripid, tablockid
    )
    values ${placeholders}
  `;

  await query(sql, values.flat());
};

const getRoutes = async () => {
  const envRoutes = optionalEnv('CTA_BUS_TRACKER_ROUTES', '').split(',').map((r) => r.trim()).filter(Boolean);
  if (envRoutes.length) return envRoutes;
  const result = await query<{ rt: string }>('select rt from bt_routes');
  return result.rows.map((row) => row.rt);
};

export const pollOnce = async () => {
  const routes = await getRoutes();
  const batches = chunk(routes, BATCH_SIZE);

  for (const batch of batches) {
    const response = await busTimeRequest<VehiclesResponse>('getvehicles', { rt: batch.join(',') });
    const vehicles = response.vehicle ?? response.vehicles ?? [];
    await insertVehicles(vehicles);
  }
};

export const runPoller = async () => {
  const intervalSec = Number(optionalEnv('CTA_BUS_TRACKER_POLL_INTERVAL_SEC', String(DEFAULT_INTERVAL)));
  let backoff = 0;

  while (true) {
    try {
      await pollOnce();
      backoff = 0;
      await sleep(intervalSec * 1000);
    } catch (error) {
      console.error('Poller failed', error);
      backoff = Math.min(backoff + 1, 6);
      const wait = Math.pow(2, backoff) * 1000;
      await sleep(wait);
    }
  }
};

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  runPoller().catch((error) => {
    console.error('Poller crashed', error);
    process.exit(1);
  });
}
