<script lang="ts">
	import '../app.css'
	import { page } from '$app/state'
	import { beforeNavigate } from '$app/navigation'
	import { fly } from 'svelte/transition'
	import { onMount } from 'svelte'
	import type { LayoutProps } from './$types'

	let { children }: LayoutProps = $props()

	let theme = $state<'light' | 'dark'>('light')

	onMount(() => {
		const current = document.documentElement.getAttribute('data-theme')
		theme = current === 'dark' ? 'dark' : 'light'
	})

	function toggleTheme() {
		theme = theme === 'dark' ? 'light' : 'dark'
		document.documentElement.setAttribute('data-theme', theme)
		try {
			localStorage.setItem('theme', theme)
		} catch (_) {}
	}

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
	<div class="header-actions">
		<button
			type="button"
			class="theme-toggle"
			onclick={toggleTheme}
			aria-label={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
			title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
		>
			{theme === 'dark' ? '☀' : '☾'}
		</button>
		<div class="badge">Development Build</div>
	</div>
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
