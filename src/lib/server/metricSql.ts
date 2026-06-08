/**
 * SQL for the headway metrics that must be derived from summed "sufficient
 * statistics" rather than from pre-aggregated per-bucket values.
 *
 * Excess wait, headway CV and the mean headways are non-linear functions of the
 * underlying headways, so they cannot be correctly recombined by averaging the
 * finished per-bucket results (a weighted mean of CVs, or of medians, is not the
 * pooled CV/median). Instead every aggregate row stores additive components —
 * counts and Σx / Σx² — and we sum those across buckets/services/dates and
 * compute the metric once. That yields the exact pooled value at any grouping.
 *
 * Component columns (present on route_bunching_stats and route_daily_bunching_stats):
 *   total_headways        — all observed headways (super-bunched denominator)
 *   analyzable_headways   — headways with a scheduled baseline and within the
 *                           sanity cap; the denominator for every schedule-relative
 *                           metric (bunching, gapping, EWT, CV, mean headways)
 *   bunched_headways / gapped_headways / super_bunched_headways — event counts
 *   sum_actual_hw / sum_actual_hw_sq — Σh and Σh² over analyzable rows
 *   sum_sched_hw  / sum_sched_hw_sq  — ΣH and ΣH² over analyzable rows
 *
 * `agg` adapts the component column to the surrounding query: pass
 * `c => `SUM(t.${c})`` when grouping, or `c => `t.${c}`` to read a single row.
 */
export const metricExpressions = (agg: (column: string) => string) => {
	const total = agg('total_headways')
	const analyzable = agg('analyzable_headways')
	const bunched = agg('bunched_headways')
	const gapped = agg('gapped_headways')
	const superBunched = agg('super_bunched_headways')
	const sumActual = agg('sum_actual_hw')
	const sumActualSq = agg('sum_actual_hw_sq')
	const sumSched = agg('sum_sched_hw')
	const sumSchedSq = agg('sum_sched_hw_sq')

	// Mean rider wait for random (turn-up-and-go) arrivals = E[H²] / (2·E[H]),
	// which equals Σh² / (2·Σh). Excess wait is the actual minus the scheduled
	// baseline computed the same way over the same rows.
	const observedWait = `${sumActualSq} / NULLIF(2 * ${sumActual}, 0)`
	const scheduledWait = `${sumSchedSq} / NULLIF(2 * ${sumSched}, 0)`

	// Sample standard deviation from sums: sqrt((Σx² − (Σx)²/n) / (n−1)).
	// GREATEST(...,0) absorbs tiny negative values from float rounding.
	const mean = `${sumActual} / NULLIF(${analyzable}, 0)`
	const stddev = `sqrt(GREATEST(${sumActualSq} - (${sumActual} * ${sumActual}) / NULLIF(${analyzable}, 0), 0) / NULLIF(${analyzable} - 1, 0))`

	return {
		bunching_rate: `${bunched}::float / NULLIF(${analyzable}, 0)`,
		super_bunching_rate: `${superBunched}::float / NULLIF(${total}, 0)`,
		gapping_rate: `${gapped}::float / NULLIF(${analyzable}, 0)`,
		mean_scheduled_headway: `${sumSched} / NULLIF(${analyzable}, 0)`,
		mean_actual_headway: `${sumActual} / NULLIF(${analyzable}, 0)`,
		excess_wait_min: `(${observedWait}) - (${scheduledWait})`,
		headway_cv: `(${stddev}) / NULLIF(${mean}, 0)`
	}
}

/** Component columns every aggregate query must SUM to feed `metricExpressions`. */
export const METRIC_COMPONENT_COLUMNS = [
	'total_headways',
	'analyzable_headways',
	'bunched_headways',
	'gapped_headways',
	'super_bunched_headways',
	'sum_actual_hw',
	'sum_actual_hw_sq',
	'sum_sched_hw',
	'sum_sched_hw_sq'
] as const
