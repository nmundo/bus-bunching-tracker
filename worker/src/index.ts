import cron from 'node-cron';
import { runSync } from './busTrackerSync';
import { runPoller } from './busTrackerPoller';
import { runArrivals } from './arrivalsProcessor';
import { runHeadways } from './headwayProcessor';
import { runEnrich } from './enrichJob';

const scheduleJobs = () => {
  cron.schedule('30 2 * * *', () => {
    runSync().catch((error) => console.error('Reference sync failed', error));
  });

  cron.schedule('*/5 * * * *', () => {
    runArrivals().catch((error) => console.error('Arrivals job failed', error));
  });

  cron.schedule('*/10 * * * *', () => {
    runHeadways().catch((error) => console.error('Headways job failed', error));
  });

  cron.schedule('15 * * * *', () => {
    runEnrich().catch((error) => console.error('Enrichment job failed', error));
  });
};

const run = async () => {
  scheduleJobs();
  await runPoller();
};

run().catch((error) => {
  console.error('Worker crashed', error);
  process.exit(1);
});
