<script lang="ts">
	import { page } from '$app/state'

	const isNotFound = $derived(page.status === 404)
	const heading = $derived(isNotFound ? 'Page not found' : 'Something went wrong')
	const detail = $derived(
		isNotFound
			? "We couldn't find that route or page. It may have moved, or never existed."
			: (page.error?.message ?? 'An unexpected error occurred. Please try again.')
	)
</script>

<section class="error-page">
	<p class="error-status mono">{page.status}</p>
	<h2>{heading}</h2>
	<p class="error-detail">{detail}</p>
	<a class="error-home" href="/">← Back to the network overview</a>
</section>

<style>
	.error-page {
		display: flex;
		flex-direction: column;
		align-items: center;
		text-align: center;
		gap: 12px;
		padding: 72px 24px;
		max-width: 480px;
		margin: 0 auto;
	}
	.error-status {
		font-size: 56px;
		font-weight: 600;
		line-height: 1;
		color: var(--text-muted);
		margin: 0;
	}
	.error-page h2 {
		margin: 0;
		font-size: 22px;
		color: var(--text-strong);
	}
	.error-detail {
		margin: 0;
		color: var(--text-muted);
	}
	.error-home {
		margin-top: 8px;
		padding: 10px 18px;
		border-radius: 8px;
		background: var(--brand);
		color: var(--brand-text);
		text-decoration: none;
		font-weight: 500;
	}
	.error-home:hover {
		background: var(--brand-hover);
	}
</style>
