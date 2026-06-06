export type RouteStat = {
	route_id: string
	route_short_name: string
	route_long_name: string | null
	bunching_rate: number | null
	total_headways: number | null
	avg_hw_ratio: number | null
	worst_bucket?: string | null
	super_bunching_rate?: number | null
	gapping_rate?: number | null
	excess_wait_min?: number | null
	headway_cv?: number | null
	median_scheduled_headway?: number | null
}

export type BucketStat = {
	hour_of_day: number
	total_headways: number
	bunching_rate: number | null
}
