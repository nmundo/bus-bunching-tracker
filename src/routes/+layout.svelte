<script lang="ts">
  import '../app.css';
</script>

<svelte:head>
  <title>CTA Bus Bunching Tracker</title>
</svelte:head>

<header class="site-header">
  <div class="brand">
    <span>CTA performance</span>
    <h1>Bus Bunching Tracker</h1>
  </div>
  <div class="badge">Live analytics</div>
</header>

<main>
  <slot />
</main>
