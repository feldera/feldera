<script lang="ts">
  import { Select } from 'common-ui'
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
      {@const tenant = authorizedTenants[0]}
      <Select value={tenant} disabled class="text-base opacity-100">
        <option value={tenant}>{tenant}</option>
      </Select>
    {:else}
      <Select
        onchange={async () => {
          if (page.url.pathname === '/') {
            invalidate(`${felderaEndpoint}/v0/pipelines?selector=status_with_connectors`)
          } else {
            goto('/')
          }
        }}
        bind:value={getSelectedTenant, setSelectedTenant}
        class="text-base"
      >
        {#each authorizedTenants as tenant}
          <option value={tenant}>{tenant}</option>
        {/each}
      </Select>
    {/if}
  </label>
{/if}
