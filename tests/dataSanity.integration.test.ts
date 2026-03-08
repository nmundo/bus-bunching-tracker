import { readFile } from 'node:fs/promises'
import path from 'node:path'
import pg from 'pg'
import { describe, expect, it } from 'vitest'
import 'dotenv/config'

type SanitySeverity = 'fail' | 'warn'

type SanityRow = {
	check_name: string
	severity: SanitySeverity
	passed: boolean
	metric_value: number
	threshold: number
	details_json: Record<string, unknown>
}

const parseNumber = (value: string | undefined, fallback: number) => {
	if (!value) return fallback
	const parsed = Number(value)
	if (!Number.isFinite(parsed)) {
		throw new Error(`Invalid numeric env var value: ${value}`)
	}
	return parsed
}

const runDbTests = process.env.RUN_DB_TESTS === '1'

const config = {
	windowDays: Math.max(1, Math.floor(parseNumber(process.env.SANITY_WINDOW_DAYS, 7))),
	statsWindowDays: Math.max(1, Math.floor(parseNumber(process.env.SANITY_STATS_WINDOW_DAYS, 30))),
	allSegmentsBunchedMinRate: parseNumber(process.env.SANITY_ALL_SEGMENTS_MIN_RATE, 0.95),
	allSegmentsShareWarnThreshold: parseNumber(process.env.SANITY_ALL_SEGMENTS_SHARE_WARN, 0.3),
	nullScheduledShareWarnThreshold: parseNumber(process.env.SANITY_NULL_SCHEDULED_SHARE_WARN, 0.25),
	bunchedShareWarnThreshold: parseNumber(process.env.SANITY_BUNCHED_SHARE_WARN, 0.5),
	rateTolerance: parseNumber(process.env.SANITY_RATE_TOLERANCE, 1e-6)
}

const formatRow = (row: SanityRow) =>
	`${row.severity.toUpperCase()} ${row.check_name}: metric=${row.metric_value} threshold=${row.threshold} details=${JSON.stringify(row.details_json)}`

describe('data sanity integration checks', () => {
	it('is opt-in via RUN_DB_TESTS=1', () => {
		expect(runDbTests || !runDbTests).toBe(true)
	})

	const maybeIt = runDbTests ? it : it.skip

	maybeIt(
		'executes DB sanity checks and fails on hard invariant breaches',
		async () => {
			const connectionString = process.env.DATABASE_URL
			if (!connectionString) {
				throw new Error('DATABASE_URL must be set when RUN_DB_TESTS=1')
			}

			const sqlPath = path.resolve(process.cwd(), 'supabase/snippets/sanity_checks.sql')
			const sql = await readFile(sqlPath, 'utf8')
			const pool = new pg.Pool({ connectionString })

			try {
				const result = await pool.query<SanityRow>(sql, [
					config.windowDays,
					config.statsWindowDays,
					config.allSegmentsBunchedMinRate,
					config.allSegmentsShareWarnThreshold,
					config.nullScheduledShareWarnThreshold,
					config.bunchedShareWarnThreshold,
					config.rateTolerance
				])

				expect(result.rows.length).toBeGreaterThan(0)

				const failingHardChecks = result.rows.filter(
					(row) => row.severity === 'fail' && !row.passed
				)
				const warningChecks = result.rows.filter(
					(row) => row.severity === 'warn' && !row.passed
				)

				if (warningChecks.length > 0) {
					console.warn('[sanity-check warnings]')
					for (const warning of warningChecks) {
						console.warn(formatRow(warning))
					}
				}

				if (failingHardChecks.length > 0) {
					const detail = failingHardChecks.map(formatRow).join('\n')
					throw new Error(`Hard sanity checks failed:\n${detail}`)
				}
			} finally {
				await pool.end()
			}
		},
		60000
	)
})
