<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { goto, invalidateAll } from '$app/navigation'
  import { page } from '$app/state'
  import { clearConfigCaches } from '$lib/compositions/configCache'
  import { resolve } from '$lib/functions/svelte'
  import { setSelectedTenant } from '$lib/services/auth'
  import { createTenant, getTenants, type Tenant } from '$lib/services/pipelineManager'

  const tenants = asyncReadable<Tenant[]>([], getTenants, { reloadable: true })

  const currentTenant = $derived(page.data.feldera?.tenantName)

  let newName = $state('')
  let newProvider = $state('')
  let creating = $state(false)
  let errorMessage = $state('')

  const create = async () => {
    if (!newName.trim()) {
      errorMessage = 'Specify a tenant name'
      return
    }
    errorMessage = ''
    creating = true
    try {
      await createTenant(newName.trim(), newProvider.trim() || undefined)
      newName = ''
      newProvider = ''
      tenants.reload?.()
    } catch (e) {
      errorMessage = e instanceof Error ? e.message : String(e)
    } finally {
      creating = false
    }
  }

  // Switch the active tenant via the `Feldera-Tenant` header. Clearing the
  // cached config/session forces the next load to re-fetch /config/session
  // under the new header (the warm cache would otherwise keep reporting the
  // previous acting tenant), then navigate home and re-run loaders so the whole
  // UI (incl. the top-right tenant indicator) reflects the new tenant.
  const actAs = async (tenant: Tenant) => {
    setSelectedTenant(tenant.name)
    clearConfigCaches()
    await goto(resolve('/'))
    await invalidateAll()
  }
</script>

<div class="flex flex-col gap-3">
  {#if errorMessage}
    <div class="rounded preset-outlined-error-600-400 p-2 text-sm">{errorMessage}</div>
  {/if}
  <div class="scrollbar flex flex-col gap-2 overflow-auto">
    {#each $tenants as tenant (tenant.id)}
      <div class="flex flex-nowrap items-center gap-2 border-b border-surface-100-900 py-2">
        <div class="w-full">
          <div class="font-medium">
            {tenant.name}
            {#if tenant.name === currentTenant}
              <span class="text-xs opacity-70">(current)</span>
            {/if}
          </div>
          <div class="text-sm opacity-70"><code>{tenant.provider}</code> · {tenant.id}</div>
        </div>
        <button
          class="btn preset-filled-surface-50-950"
          disabled={tenant.name === currentTenant}
          onclick={() => actAs(tenant)}
        >
          Act as this tenant
        </button>
      </div>
    {:else}
      <div class="opacity-70">No tenants found</div>
    {/each}
  </div>

  <form
    class="flex items-end gap-2"
    onsubmit={(e) => {
      e.preventDefault()
      create()
    }}
  >
    <label class="label w-full">
      <span>New tenant name</span>
      <input class="input w-full" placeholder="acme-prod" bind:value={newName} />
    </label>
    <label class="label w-48">
      <span>Provider (optional)</span>
      <input class="input w-full" placeholder="GenericOidc" bind:value={newProvider} />
    </label>
    <button class="btn preset-filled-surface-50-950" disabled={creating}>Create</button>
  </form>
</div>
