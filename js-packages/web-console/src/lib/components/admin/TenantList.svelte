<script lang="ts">
  import { asyncReadable, type Loadable } from '@square/svelte-store'
  import { goto, invalidateAll } from '$app/navigation'
  import { page } from '$app/state'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import DoubleClickInput from '$lib/components/input/DoubleClickInput.svelte'
  import { clearConfigCaches } from '$lib/compositions/configCache'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { resolve } from '$lib/functions/svelte'
  import { setSelectedTenant } from '$lib/services/auth'
  import {
    createTenant,
    deleteTenant,
    getAuthConfig,
    renameTenant,
    type Tenant
  } from '$lib/services/pipelineManager'

  const { tenants }: { tenants: Loadable<Tenant[]> } = $props()

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
  const currentTenantId = $derived(page.data.feldera!.tenantId)

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

  let renaming = $state(false)

  // Set when a rename conflicts, to offer taking the name from the tenant that
  // holds it. Every request re-creates the name its token resolves, so a
  // conflict is the normal case when recovering a tenant no login reaches.
  let conflict = $state<{ tenantId: string; name: string } | null>(null)

  const rename = async (tenantId: string, rawName: string, displaceExisting = false) => {
    const name = rawName.trim()
    if (!name) {
      errorMessage = 'Specify a tenant name'
      return
    }
    errorMessage = ''
    noticeMessage = ''
    conflict = null
    renaming = true
    try {
      const { displaced } = await renameTenant(tenantId, name, displaceExisting)
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
        conflict = { tenantId, name }
      } else {
        errorMessage = message
      }
    } finally {
      renaming = false
    }
  }

  const globalDialog = useGlobalDialog()

  // Deletion is refused unless the tenant is empty, so the confirmation is
  // about intent, not about losing data.
  const remove = async (tenant: Tenant) => {
    errorMessage = ''
    noticeMessage = ''
    try {
      await deleteTenant(tenant.id)
      noticeMessage = `Deleted tenant '${tenant.name}'.`
      tenants.reload?.()
    } catch (e) {
      errorMessage = e instanceof Error ? e.message : String(e)
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
  <div class="scrollbar flex flex-col gap-2 overflow-visible">
    {#each $tenants as tenant (tenant.id)}
      <div class="flex flex-wrap items-center gap-2 border-b border-surface-200-800 py-2">
        <div class="">
          <!-- Rename in place: the input sits where the name was, so editing
               reads as editing the name rather than filling a form. -->
          <DoubleClickInput
            value={tenant.name}
            onvalue={(name) => rename(tenant.id, name)}
            editLabel="Edit tenant name"
            class="flex h-5 flex-nowrap gap-1"
            inputClass="input font-medium -ml-1 w-72 h-5 pl-1"
          >
            <div class="font-medium">{tenant.name}</div>
            {#if tenant.id === currentTenantId}
              <span class="text-sm text-surface-800-200">(current)</span>
            {/if}
          </DoubleClickInput>
          {#if conflict?.tenantId === tenant.id}
            {@const pending = conflict!}
            <div class="mt-2 rounded preset-outlined-warning-600-400 p-2 text-sm">
              Another tenant is named <code>{pending.name}</code>. Taking the name renames that
              tenant to <code>{pending.name} (its id)</code>; it keeps its pipelines, keys and
              members. Logins that resolve <code>{pending.name}</code> then land here.
              <button
                class="ml-2 btn preset-filled-surface-50-950"
                disabled={renaming}
                onclick={() => rename(tenant.id, pending.name, true)}
              >
                Take the name
              </button>
            </div>
          {/if}
          <!-- The issuer is provenance, not something a login resolves, so it
               stays out of the row and is available on hover. -->
          <div
            class="text-sm text-surface-800-200"
            title="First provisioned under {tenant.initial_provider}"
          >
            {tenant.id}
          </div>
        </div>
        {#snippet deleteDialog()}
          <DeleteDialog
            {...deleteDialogProps(
              'Delete',
              `Delete tenant '${tenant.name}'?`,
              () => remove(tenant),
              'Only an empty tenant can be deleted: no pipelines, API keys or OIDC trust relationships.'
            )()}
          ></DeleteDialog>
        {/snippet}
        <div class="ml-auto flex flex-nowrap gap-2">
          <button
            class="btn preset-filled-surface-50-950"
            disabled={tenant.id === currentTenantId}
            onclick={() => actAs(tenant)}
          >
            Act as this tenant
          </button>
          <button
            class="fd fd-trash-2 btn-icon text-[20px] hover:bg-surface-50-950"
            disabled={tenant.id === currentTenantId}
            onclick={() => (globalDialog.dialog = deleteDialog)}
            aria-label="Delete tenant {tenant.name}"
            title="Only an empty tenant can be deleted: no pipelines, API keys or OIDC trust relationships."
          ></button>
        </div>
      </div>
    {:else}
      <div class="text-surface-800-200">No tenants found</div>
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
      <input class="input h-9 w-full" placeholder="acme-prod" bind:value={newName} />
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
