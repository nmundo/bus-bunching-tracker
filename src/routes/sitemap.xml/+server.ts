import type { RequestHandler } from './$types'
import { query } from '$server/db'

// Built from the live route list so newly-tracked routes appear automatically.
export const prerender = false

const xmlEscape = (s: string) =>
	s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')

export const GET: RequestHandler = async ({ url }) => {
	const origin = url.origin

	let routeIds: string[] = []
	try {
		const res = await query<{ route_id: string }>(
			`SELECT DISTINCT r.route_id
			 FROM gtfs_routes r
			 JOIN route_bunching_stats rbs ON rbs.route_id = r.route_id
			 ORDER BY r.route_id`
		)
		routeIds = res.rows.map((r) => r.route_id)
	} catch {
		// If the DB is unreachable, still emit a valid sitemap with the homepage.
		routeIds = []
	}

	const entries = [
		{ loc: `${origin}/`, priority: '1.0' },
		...routeIds.map((id) => ({
			loc: `${origin}/route/${encodeURIComponent(id)}`,
			priority: '0.7'
		}))
	]

	const body =
		`<?xml version="1.0" encoding="UTF-8"?>\n` +
		`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n` +
		entries
			.map(
				(e) =>
					`	<url>\n		<loc>${xmlEscape(e.loc)}</loc>\n` +
					`		<changefreq>daily</changefreq>\n		<priority>${e.priority}</priority>\n	</url>`
			)
			.join('\n') +
		`\n</urlset>\n`

	return new Response(body, {
		headers: {
			'Content-Type': 'application/xml; charset=utf-8',
			'Cache-Control': 'public, max-age=3600'
		}
	})
}
