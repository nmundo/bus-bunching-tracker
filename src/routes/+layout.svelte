<script lang="ts">
	import '../app.css'
	import { page } from '$app/state'
	import { beforeNavigate } from '$app/navigation'
	import { fly } from 'svelte/transition'
	import type { LayoutProps } from './$types'

	let { children }: LayoutProps = $props()

	// 1 = navigating deeper (home → detail), -1 = navigating back (detail → home).
	// Plain (non-reactive) so the transition param object isn't tracked — Svelte
	// re-reads it when the keyed block mounts the next page.
	let direction = $state(1)

	const isDetail = (pathname: string) => pathname.startsWith('/route/')

	beforeNavigate(({ from, to }) => {
		const fromDetail = isDetail(from?.url.pathname ?? '')
		const toDetail = isDetail(to?.url.pathname ?? '')
		direction = !fromDetail && toDetail ? 1 : -1
	})
</script>

<svelte:head>
	<title>CTA Bus Bunching Tracker</title>
</svelte:head>

<header class="site-header">
	<div class="brand">
		<span>CTA performance</span>
		<h1>Bus Bunching Tracker</h1>
	</div>
	<div class="badge">Development Build</div>
</header>

<main>
	{#key page.url.pathname}
		<div
			class="page-transition"
			in:fly={{ x: direction * 32, duration: 300, opacity: 0 }}
			out:fly={{ x: -direction * 32, duration: 220, opacity: 0 }}
		>
			{@render children()}
		</div>
	{/key}
</main>
