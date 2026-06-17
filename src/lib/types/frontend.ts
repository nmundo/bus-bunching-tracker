export type RouteStat = {
	route_id: string
	route_short_name: string
	route_long_name: string | null
	bunching_rate: number | null
	total_headways: number | null
	worst_bucket?: string | null
	super_bunching_rate?: number | null
	gapping_rate?: number | null
	excess_wait_min?: number | null
	headway_cv?: number | null
	mean_scheduled_headway?: number | null
	// Summed sufficient statistics, carried through so network-level metrics can be
	// pooled exactly (Σ components) instead of averaging per-route results. See
	// $server/metricSql.
	analyzable_headways?: number | null
	bunched_headways?: number | null
	super_bunched_headways?: number | null
	gapped_headways?: number | null
	sum_actual_hw?: number | null
	sum_actual_hw_sq?: number | null
	sum_sched_hw?: number | null
	sum_sched_hw_sq?: number | null
	// Frequent-only (scheduled headway ≤ EWT_FREQUENT_HEADWAY_MAX) sums backing
	// excess wait; pooled separately so the network EWT isn't inflated by infrequent
	// service. See $lib/ui/networkMetrics and $server/metricSql.
	ewt_analyzable_headways?: number | null
	ewt_sum_actual_hw?: number | null
	ewt_sum_actual_hw_sq?: number | null
	ewt_sum_sched_hw?: number | null
	ewt_sum_sched_hw_sq?: number | null
}

export type BucketStat = {
	hour_of_day: number
	total_headways: number
	bunching_rate: number | null
}
