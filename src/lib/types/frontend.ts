export type RouteStat = {
	route_id: string
	route_short_name: string
	route_long_name: string | null
	bunching_rate: number | null
	total_headways: number | null
	avg_hw_ratio: number | null
}

export type BucketStat = {
	time_of_day_bucket: string
	bunching_rate: number | null
}
