<script lang="ts">
  import { goto, invalidate } from '$app/navigation'
  import { page } from '$app/state'
  import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'
  import { getSelectedTenant, setSelectedTenant } from '$lib/services/auth'

  let { class: className = '' }: { class?: string } = $props()

  const authorizedTenants = page.data.feldera
    ? (page.data.feldera.authorizedTenants ?? [page.data.feldera.tenantName])
    : []
</script>

{#if page.data.feldera}
  <label class="label {className}">
    <span class="text-left">Tenant</span>
    {#if authorizedTenants.length === 1}
      <div class="h-9 rounded-md border border-surface-500 px-3 py-2 font-normal">
        {authorizedTenants[0]}
      </div>
    {:else}
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
    {/if}
  </label>
{/if}
