<script lang="ts">
	import '../app.css'
	import { page } from '$app/stores'
	import { fade, fly } from 'svelte/transition'
	import type { LayoutProps } from './$types'

	let { children }: LayoutProps = $props()
</script>

<svelte:head>
	<title>CTA Bus Bunching Tracker</title>
</svelte:head>

<header class="site-header">
	<div class="brand">
		<span>CTA performance</span>
		<h1>Bus Bunching Tracker</h1>
	</div>
</header>

<main>
	{#key $page.url.pathname}
		<div
			class="page-transition"
			in:fly={{ y: 22, duration: 260, opacity: 0.2 }}
			out:fade={{ duration: 170 }}
		>
			{@render children()}
		</div>
	{/key}
</main>
