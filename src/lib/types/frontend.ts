export type RouteStat = {
	route_id: string
	route_short_name: string
	route_long_name: string | null
	bunching_rate: number | null
	total_headways: number | null
	avg_hw_ratio: number | null
}

export type BucketStat = {
	hour_of_day: number
	total_headways: number
	bunching_rate: number | null
}
