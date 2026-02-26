<script lang="ts">
	import '../app.css'
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
	{@render children()}
</main>
