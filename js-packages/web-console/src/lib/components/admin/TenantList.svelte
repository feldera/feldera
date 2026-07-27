<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { goto, invalidateAll } from '$app/navigation'
  import { page } from '$app/state'
  import { clearConfigCaches } from '$lib/compositions/configCache'
  import { resolve } from '$lib/functions/svelte'
  import { setSelectedTenant } from '$lib/services/auth'
  import {
    createTenant,
    getAuthConfig,
    getTenants,
    type Tenant
  } from '$lib/services/pipelineManager'

  const tenants = asyncReadable<Tenant[]>([], getTenants, { reloadable: true })

  // A login resolves its tenant by name, so the name a new tenant is given is
  // the one the identity provider must assert for its users to land in it. The
  // issuer is configured at deploy time, not here; show it read-only.
  const authConfig = asyncReadable<Record<string, { issuer?: string }> | undefined>(
    undefined,
    getAuthConfig
  )
  const configuredIssuer = $derived(
    $authConfig?.GenericOidc?.issuer ??
      $authConfig?.AwsCognito?.issuer ??
      '(configured at deploy time)'
  )

  // Compare/select by tenant id (UUID): the id never changes, and the backend's
  // Feldera-Tenant resolver accepts it unambiguously.
  const currentTenantId = $derived(page.data.feldera?.tenantId)

  let newName = $state('')
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
      await createTenant(newName.trim())
      newName = ''
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
    setSelectedTenant(tenant.id)
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
            {#if tenant.id === currentTenantId}
              <span class="text-xs opacity-70">(current)</span>
            {/if}
          </div>
          <div class="text-sm opacity-70"><code>{tenant.initial_provider}</code> · {tenant.id}</div>
        </div>
        <button
          class="btn preset-filled-surface-50-950"
          disabled={tenant.id === currentTenantId}
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
    <label class="label w-72">
      <span>OIDC issuer</span>
      <input
        class="input w-full opacity-60"
        value={configuredIssuer}
        readonly
        disabled
        title="Statically configured at deploy time (Helm: FELDERA_AUTH_ISSUER). Recorded on a new tenant as the issuer it was provisioned under; it cannot be changed here."
      />
    </label>
    <button class="btn preset-filled-surface-50-950" disabled={creating}>Create</button>
  </form>
</div>
