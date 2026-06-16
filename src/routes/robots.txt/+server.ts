import type { RequestHandler } from './$types'

// Served dynamically so the Sitemap URL always matches the deployed origin.
export const prerender = false

export const GET: RequestHandler = ({ url }) => {
	const body = `User-agent: *\nAllow: /\n\nSitemap: ${url.origin}/sitemap.xml\n`
	return new Response(body, {
		headers: {
			'Content-Type': 'text/plain; charset=utf-8',
			'Cache-Control': 'public, max-age=86400'
		}
	})
}
