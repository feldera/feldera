<script lang="ts">
  import { invalidateAll } from '$app/navigation'
  import { page } from '$app/state'
  import { getSelectedTenant, setSelectedTenant } from '$lib/services/auth'

  const authorizedTenants = page.data.feldera ? (page.data.feldera.authorizedTenants ?? [page.data.feldera.tenantName]) : []
</script>

{#if page.data.feldera}
  <label class="label">
    <span class="text-left">Tenant</span>
    <select onchange={() => invalidateAll()} bind:value={getSelectedTenant, setSelectedTenant} class="select {authorizedTenants.length > 1 ? '' : 'pointer-events-none'}">
      {#each authorizedTenants as tenant}
        <option value={tenant}>{tenant}</option>
      {/each}
    </select>
  </label>
{/if}
