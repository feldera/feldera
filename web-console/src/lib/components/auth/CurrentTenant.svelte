<script lang="ts">
  import { goto, invalidate } from '$app/navigation'
  import { page } from '$app/state'
  import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'
  import { getSelectedTenant, setSelectedTenant } from '$lib/services/auth'

  const authorizedTenants = page.data.feldera
    ? (page.data.feldera.authorizedTenants ?? [page.data.feldera.tenantName])
    : []
</script>

{#if page.data.feldera}
  <label class="label">
    <span class="text-left">Tenant</span>
    <select
      onchange={async () => {
        if (page.url.pathname === '/') {
          invalidate(`${felderaEndpoint}/v0/pipelines?selector=status_with_connectors`)
        } else {
          goto('/')
        }
      }}
      bind:value={getSelectedTenant, setSelectedTenant}
      class="select {authorizedTenants.length > 1 ? '' : 'pointer-events-none'}"
    >
      {#each authorizedTenants as tenant}
        <option value={tenant}>{tenant}</option>
      {/each}
    </select>
  </label>
{/if}
