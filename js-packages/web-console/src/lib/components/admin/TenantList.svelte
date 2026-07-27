<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { goto, invalidateAll } from '$app/navigation'
  import { page } from '$app/state'
  import { clearConfigCaches } from '$lib/compositions/configCache'
  import { resolve } from '$lib/functions/svelte'
  import { setSelectedTenant } from '$lib/services/auth'
  import {
    createTenant,
    deleteTenant,
    getAuthConfig,
    getTenants,
    renameTenant,
    type Tenant
  } from '$lib/services/pipelineManager'

  const tenants = asyncReadable<Tenant[]>([], getTenants, { reloadable: true })

  // A login resolves its tenant by name, so the name a new tenant is given is
  // the one the identity provider must assert for its users to land in it. The
  // issuer only gets recorded as provenance, so it lives in a hover rather than
  // a field of its own.
  const authConfig = asyncReadable<Record<string, { issuer?: string }> | undefined>(
    undefined,
    getAuthConfig
  )
  const configuredIssuer = $derived(
    $authConfig?.GenericOidc?.issuer ??
      $authConfig?.AwsCognito?.issuer ??
      '(configured at deploy time)'
  )

  // Compare/select by tenant id (UUID): a name can be reassigned by a rename,
  // the id never changes, and the backend's Feldera-Tenant resolver accepts it.
  const currentTenantId = $derived(page.data.feldera?.tenantId)

  let newName = $state('')
  let creating = $state(false)
  let errorMessage = $state('')
  let noticeMessage = $state('')

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

  let renamingId = $state('')
  let renamedName = $state('')
  let renaming = $state(false)

  const startRename = (tenant: Tenant) => {
    errorMessage = ''
    noticeMessage = ''
    nameToTakeOver = ''
    renamingId = tenant.id
    renamedName = tenant.name
  }

  const cancelRename = () => {
    renamingId = ''
    renamedName = ''
    nameToTakeOver = ''
  }

  // Set when a rename conflicts, to offer taking the name from the tenant that
  // holds it. Every request re-creates the name its token resolves, so a
  // conflict is the normal case when recovering a tenant no login reaches.
  let nameToTakeOver = $state('')

  const submitRename = async (displaceExisting = false) => {
    const name = renamedName.trim()
    if (!name) {
      errorMessage = 'Specify a tenant name'
      return
    }
    errorMessage = ''
    noticeMessage = ''
    nameToTakeOver = ''
    renaming = true
    try {
      const { displaced } = await renameTenant(renamingId, name, displaceExisting)
      cancelRename()
      if (displaced) {
        noticeMessage = `Took the name '${name}'. The tenant that held it is now '${displaced.name}' and keeps everything it had.`
      }
      tenants.reload?.()
      // The tenant indicator and any loader that resolved the old name still
      // hold it, so re-fetch once the rename lands.
      clearConfigCaches()
      await invalidateAll()
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e)
      if (/already exists|DuplicateName/i.test(message) && !displaceExisting) {
        nameToTakeOver = name
      } else {
        errorMessage = message
      }
    } finally {
      renaming = false
    }
  }

  // Deletion is refused unless the tenant is empty, so the confirmation is
  // about intent, not about losing data.
  let tenantToDelete = $state('')
  let deleting = $state(false)

  const confirmDelete = async (tenant: Tenant) => {
    errorMessage = ''
    noticeMessage = ''
    if (tenantToDelete !== tenant.id) {
      tenantToDelete = tenant.id
      return
    }
    deleting = true
    try {
      await deleteTenant(tenant.id)
      tenantToDelete = ''
      noticeMessage = `Deleted tenant '${tenant.name}'.`
      tenants.reload?.()
    } catch (e) {
      tenantToDelete = ''
      errorMessage = e instanceof Error ? e.message : String(e)
    } finally {
      deleting = false
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
  {#if noticeMessage}
    <div class="rounded preset-outlined-success-600-400 p-2 text-sm">{noticeMessage}</div>
  {/if}
  <div class="scrollbar flex flex-col gap-2 overflow-auto">
    {#each $tenants as tenant (tenant.id)}
      <div class="flex flex-nowrap items-center gap-2 border-b border-surface-100-900 py-2">
        <div class="w-full">
          {#if renamingId === tenant.id}
            <form
              class="flex items-center gap-2"
              onsubmit={(e) => {
                e.preventDefault()
                submitRename()
              }}
            >
              <!-- svelte-ignore a11y_autofocus -->
              <input class="input w-72" bind:value={renamedName} autofocus />
              <button class="btn preset-filled-surface-50-950" disabled={renaming}>Save</button>
              <button
                type="button"
                class="btn preset-tonal-surface"
                disabled={renaming}
                onclick={cancelRename}
              >
                Cancel
              </button>
            </form>
            {#if nameToTakeOver}
              <div class="mt-2 rounded preset-outlined-warning-600-400 p-2 text-sm">
                Another tenant is named <code>{nameToTakeOver}</code>. Taking the name renames that
                tenant to <code>{nameToTakeOver} (its id)</code>; it keeps its pipelines, keys and
                members. Logins that resolve <code>{nameToTakeOver}</code> then land here.
                <button
                  class="ml-2 btn preset-filled-surface-50-950"
                  disabled={renaming}
                  onclick={() => submitRename(true)}
                >
                  Take the name
                </button>
              </div>
            {/if}
          {:else}
            <div class="font-medium">
              {tenant.name}
              {#if tenant.id === currentTenantId}
                <span class="text-xs opacity-70">(current)</span>
              {/if}
            </div>
          {/if}
          <!-- The issuer is provenance, not something a login resolves, so it
               stays out of the row and is available on hover. -->
          <div class="text-sm opacity-70" title="First provisioned under {tenant.initial_provider}">
            {tenant.id}
          </div>
        </div>
        <button
          class="btn preset-tonal-surface"
          disabled={renamingId === tenant.id}
          onclick={() => startRename(tenant)}
          title="A login resolves its tenant by name: renaming changes which users land in this tenant."
        >
          Rename
        </button>
        <button
          class="btn preset-filled-surface-50-950"
          disabled={tenant.id === currentTenantId}
          onclick={() => actAs(tenant)}
        >
          Act as this tenant
        </button>
        <button
          class="btn {tenantToDelete === tenant.id
            ? 'preset-filled-error-500'
            : 'preset-tonal-surface'}"
          disabled={deleting || tenant.id === currentTenantId}
          onclick={() => confirmDelete(tenant)}
          title="Only an empty tenant can be deleted: no pipelines, API keys or OIDC trust relationships."
        >
          {tenantToDelete === tenant.id ? 'Confirm delete' : 'Delete'}
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
    <button
      class="btn preset-filled-surface-50-950"
      disabled={creating}
      title="Records {configuredIssuer} as the issuer the tenant was provisioned under. A login reaches the tenant by name, whatever the issuer."
    >
      Create
    </button>
  </form>
</div>
