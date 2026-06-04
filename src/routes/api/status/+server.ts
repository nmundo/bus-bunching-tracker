import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { query } from '$server/db'

export const GET: RequestHandler = async () => {
	// Serving DB uses publish_meta; warehouse DB uses job_state.
	let watermark: string | null = null
	try {
		const r = await query<{ ts: string }>(
			`SELECT last_published_at AS ts FROM publish_meta WHERE id = 1`,
			[]
		)
		watermark = r.rows[0]?.ts ?? null
	} catch {
		/* not serving DB — try job_state */
	}
	if (!watermark) {
		try {
			const r = await query<{ ts: string }>(
				`SELECT watermark AS ts FROM job_state ORDER BY watermark DESC LIMIT 1`,
				[]
			)
			watermark = r.rows[0]?.ts ?? null
		} catch {
			/* no watermark available */
		}
	}
	return json({ watermark })
}
