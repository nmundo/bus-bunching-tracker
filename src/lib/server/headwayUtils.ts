export type Arrival = {
	vid: string
	arrival_time: Date
}

export type Headway = {
	prev_vid: string
	curr_vid: string
	arrival_time: Date
	headway_min: number
}

export const computeHeadways = (arrivals: Arrival[]): Headway[] => {
	const sorted = [...arrivals].sort((a, b) => a.arrival_time.getTime() - b.arrival_time.getTime())
	const results: Headway[] = []
	for (let i = 1; i < sorted.length; i += 1) {
		const prev = sorted[i - 1]
		const curr = sorted[i]
		const headwayMin = (curr.arrival_time.getTime() - prev.arrival_time.getTime()) / 60000
		results.push({
			prev_vid: prev.vid,
			curr_vid: curr.vid,
			arrival_time: curr.arrival_time,
			headway_min: headwayMin
		})
	}
	return results
}

export const classifyBunching = (actual: number, scheduled: number | null) => {
	if (!scheduled || scheduled <= 0) {
		return {
			hw_ratio: null,
			bunched: false,
			super_bunched: actual <= 1.0,
			gapped: false
		}
	}

	const hw_ratio = actual / scheduled
	return {
		hw_ratio,
		bunched: actual < 0.25 * scheduled,
		super_bunched: actual <= 1.0,
		gapped: actual > 1.75 * scheduled
	}
}
