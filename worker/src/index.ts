import cron from 'node-cron'
import { runSync } from './busTrackerSync'
import { runPoller } from './busTrackerPoller'
import { runArrivals } from './arrivalsProcessor'
import { runHeadways } from './headwayProcessor'
import { runEnrich } from './enrichJob'
import { runDailySnapshot } from './dailySnapshotJob'
import { runPublishServing } from './publishServing'

const scheduleJobs = () => {
	// Each flag prevents a second instance of the same job from starting if the
	// previous run is still in progress (e.g. after a large backlog).
	let syncRunning = false
	let arrivalsRunning = false
	let headwaysRunning = false
	let enrichRunning = false
	let dailySnapshotRunning = false
	let publishRunning = false

	cron.schedule('30 2 * * *', () => {
		if (syncRunning) {
			console.warn('runSync already in progress, skipping this tick')
			return
		}
		syncRunning = true
		runSync()
			.catch((error) => console.error('Reference sync failed', error))
			.finally(() => {
				syncRunning = false
			})
	})

	cron.schedule('*/5 * * * *', () => {
		if (arrivalsRunning) {
			console.warn('runArrivals already in progress, skipping this tick')
			return
		}
		arrivalsRunning = true
		runArrivals()
			.catch((error) => console.error('Arrivals job failed', error))
			.finally(() => {
				arrivalsRunning = false
			})
	})

	cron.schedule('*/10 * * * *', () => {
		if (headwaysRunning) {
			console.warn('runHeadways already in progress, skipping this tick')
			return
		}
		headwaysRunning = true
		runHeadways()
			.catch((error) => console.error('Headways job failed', error))
			.finally(() => {
				headwaysRunning = false
			})
	})

	cron.schedule('15 * * * *', () => {
		if (enrichRunning) {
			console.warn('runEnrich already in progress, skipping this tick')
			return
		}
		enrichRunning = true
		runEnrich()
			.catch((error) => console.error('Enrichment job failed', error))
			.finally(() => {
				enrichRunning = false
			})
	})

	// Snapshot the previous local day just before the publish below picks it up.
	cron.schedule(
		'15 3 * * *',
		() => {
			if (dailySnapshotRunning) {
				console.warn('runDailySnapshot already in progress, skipping this tick')
				return
			}
			dailySnapshotRunning = true
			runDailySnapshot()
				.catch((error) => console.error('Daily snapshot failed', error))
				.finally(() => {
					dailySnapshotRunning = false
				})
		},
		{ timezone: 'America/Chicago' }
	)

	cron.schedule(
		'30 3 * * *',
		() => {
			if (publishRunning) {
				console.warn('runPublishServing already in progress, skipping this tick')
				return
			}
			publishRunning = true
			runPublishServing()
				.catch((error) => console.error('Serving publish failed', error))
				.finally(() => {
					publishRunning = false
				})
		},
		{ timezone: 'America/Chicago' }
	)
}

const run = async () => {
	scheduleJobs()
	await runPoller()
}

run().catch((error) => {
	console.error('Worker crashed', error)
	process.exit(1)
})
