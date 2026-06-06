<script lang="ts">
	import '../app.css'
	import { page } from '$app/state'
	import { beforeNavigate } from '$app/navigation'
	import { fly, fade } from 'svelte/transition'
	import { onMount, tick } from 'svelte'
	import type { LayoutProps } from './$types'

	let { children }: LayoutProps = $props()

	let theme = $state<'light' | 'dark'>('light')
	let infoOpen = $state(false)
	let infoButtonEl = $state<HTMLButtonElement | null>(null)
	let modalCloseEl = $state<HTMLButtonElement | null>(null)
	let modalEl = $state<HTMLDivElement | null>(null)

	const emailParts = ['contact', 'nathanmundo.com']
	const email = $derived(emailParts.join('@'))
	const mailtoHref = $derived(`mailto:${email}`)

	$effect(() => {
		if (infoOpen) {
			tick().then(() => modalCloseEl?.focus())
		} else {
			infoButtonEl?.focus()
		}
	})

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

	function closeOnEsc(e: KeyboardEvent) {
		if (e.key === 'Escape') infoOpen = false
	}

	function closeOnOutsideClick(e: PointerEvent) {
		if (infoOpen && modalEl && !modalEl.contains(e.target as Node)) infoOpen = false
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

<svelte:window onkeydown={closeOnEsc} onpointerdown={closeOnOutsideClick} />

<header class="site-header">
	<div class="brand">
		<span>CTA performance</span>
		<h1>Bus Bunching Tracker</h1>
	</div>
	<div class="header-actions">
		<button
			type="button"
			class="theme-toggle"
			bind:this={infoButtonEl}
			onclick={() => (infoOpen = true)}
			aria-label="About this tracker"
			aria-haspopup="dialog"
			aria-expanded={infoOpen}
			title="About this tracker"
		>
			?
		</button>
		<button
			type="button"
			class="theme-toggle"
			onclick={toggleTheme}
			aria-label={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
			title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
		>
			{theme === 'dark' ? '☀' : '☾'}
		</button>
		<div class="badge" role="status">Development Build</div>
	</div>
</header>

{#if infoOpen}
	<div class="modal-outer">
		<div class="modal-backdrop" aria-hidden="true" transition:fade={{ duration: 200 }}></div>
		<div
			class="modal"
			bind:this={modalEl}
			role="dialog"
			aria-modal="true"
			aria-labelledby="modal-title"
			transition:fly={{ y: 16, duration: 250, opacity: 0 }}
		>
			<button
				class="modal-close"
				bind:this={modalCloseEl}
				onclick={() => (infoOpen = false)}
				aria-label="Close dialog">✕</button
			>

			<h2 id="modal-title" class="modal-title">About this tracker</h2>

			<section class="modal-section" aria-labelledby="modal-section-methodology">
				<h3 id="modal-section-methodology">Methodology</h3>
				<p>
					Vehicle position data is fetched from the CTA's real-time API every 30 seconds. For each
					stop on each route, consecutive bus arrivals are recorded and the <strong>headway</strong> —
					the time gap between them — is computed. Those headways are then classified and aggregated by
					time-of-day bucket (AM Peak, Midday, PM Peak, Evening, Night).
				</p>
				<p>
					Bunching thresholds are applied relative to each route's scheduled headway so a frequent
					route (every 5 min) and an infrequent one (every 20 min) are judged on their own terms.
				</p>
			</section>

			<section class="modal-section" aria-labelledby="modal-section-stats">
				<h3 id="modal-section-stats">Stats explained</h3>
				<dl class="stat-glossary">
					<div class="glossary-row">
						<dt>Bunching rate</dt>
						<dd>
							The share of observed headways where a bus arrived less than 25% of the route's
							scheduled headway after the previous bus — i.e. two buses running nearly back-to-back.
						</dd>
					</div>
					<div class="glossary-row">
						<dt>Super-bunched rate</dt>
						<dd>
							The share of observed headways of 1 minute or less — buses that are essentially
							travelling together regardless of the schedule.
						</dd>
					</div>
					<div class="glossary-row">
						<dt>Gapping rate</dt>
						<dd>
							The share of observed headways that exceed 175% of the scheduled headway — long gaps
							where passengers are left waiting much longer than expected.
						</dd>
					</div>
					<div class="glossary-row">
						<dt>Network avg bunching</dt>
						<dd>
							The bunching rate averaged across all active routes, weighted by the number of headway
							observations on each route so busier routes carry more influence.
						</dd>
					</div>
					<div class="glossary-row">
						<dt>High-risk routes</dt>
						<dd>Routes whose bunching rate is at or above 20%.</dd>
					</div>
					<div class="glossary-row">
						<dt>Median scheduled / actual headway</dt>
						<dd>
							The midpoint planned gap vs. the midpoint observed gap between buses on a route, in
							minutes. A large difference suggests the schedule is not being followed.
						</dd>
					</div>
				</dl>
			</section>

			<section class="modal-section modal-contact" aria-label="Contact">
				<p>
					Found a bug or have a suggestion? Email me at
					<a href={mailtoHref}>{email}</a> — I'd love to hear from you.
				</p>
			</section>
		</div>
	</div>
{/if}

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
