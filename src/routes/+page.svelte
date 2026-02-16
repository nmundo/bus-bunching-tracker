<script lang="ts">
  import RouteTable from '$components/RouteTable.svelte';
   import type { RouteStat } from '$components/RouteTable.svelte';

  export let data: {
    routes: RouteStat[];
    serviceId: string;
    bucket: string;
  };

  let serviceId = data.serviceId;
  let bucket = data.bucket;
  let routes = data.routes;
  let loading = false;

  const timeBuckets = ['AM_peak', 'Midday', 'PM_peak', 'Evening', 'Night'];

  const refresh = async () => {
    loading = true;
    const params = new URLSearchParams();
    if (serviceId) params.set('service_id', serviceId);
    if (bucket) params.set('time_of_day_bucket', bucket);
    const res = await fetch(`/api/routes?${params.toString()}`);
    routes = res.ok ? await res.json() : [];
    loading = false;
  };
</script>

<section class="grid">
  <div class="panel">
    <h2>Network overview</h2>
    <p>Scan which routes are struggling with bus bunching and drill into the worst segments.</p>
    <div class="controls">
      <label>
        Service ID
        <input placeholder="WKDY" bind:value={serviceId} />
      </label>
      <label>
        Time bucket
        <select bind:value={bucket}>
          {#each timeBuckets as option}
            <option value={option}>{option}</option>
          {/each}
        </select>
      </label>
      <button on:click={refresh} disabled={loading}>Refresh</button>
      {#if loading}
        <small class="mono">Loading…</small>
      {/if}
    </div>
  </div>

  <div class="panel">
    <RouteTable {routes} />
  </div>
</section>
