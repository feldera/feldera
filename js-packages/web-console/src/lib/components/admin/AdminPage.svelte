<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { Select } from 'common-ui'
  import { page } from '$app/state'
  import TenantList from '$lib/components/admin/TenantList.svelte'
  import UserRoleTable from '$lib/components/admin/UserRoleTable.svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import NewOidcTrustForm from '$lib/components/oidcTrust/NewOidcTrustForm.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import {
    deleteOidcTrust,
    getOidcTrustList,
    getTenants,
    type OidcTrustDescr,
    type Tenant
  } from '$lib/services/pipelineManager'
  import type { Snippet } from '$lib/types/svelte'

  const isOwner = $derived(page.data.feldera?.isOwner ?? false)
  const globalDialog = useGlobalDialog()
  let errorMessage = $state('')

  // Owner-only: pick a tenant (by UUID) to inspect its members in place, without
  // changing the global acting-tenant. Empty string means the current tenant.
  let adminTenant = $state(page.data.feldera?.tenantId ?? '')
  const selectedTenant = $derived(adminTenant || undefined)
  const tenants = asyncReadable<Tenant[]>([], getTenants, { reloadable: true })
  const tenantLabel = $derived(
    $tenants.find((t) => t.id === adminTenant)?.name ??
      page.data.feldera?.tenantName ??
      'current tenant'
  )

  // Owner (platform-wide) trusts belong to no tenant, so they are fetched with
  // the platform scope and are independent of the tenant switcher. Per-tenant
  // trusts are managed from the "Manage OIDC trust" menu, not here.
  const ownerTrusts = asyncReadable<OidcTrustDescr[]>([], () => getOidcTrustList(undefined, true), {
    reloadable: true
  })
</script>

{#snippet section(title: string, description: string, body: Snippet)}
  <section class="flex flex-col gap-3 rounded-container bg-surface-50-950 p-4 md:p-6">
    <div>
      <h2 class="h3">{title}</h2>
      <p class="text-sm opacity-70">{description}</p>
    </div>
    {@render body()}
  </section>
{/snippet}

<div class="mx-auto flex w-full max-w-4xl flex-col gap-6 px-2 pb-10 md:px-8">
  <h1 class="h2">Administration</h1>

  {#if errorMessage}
    <div class="rounded preset-outlined-error-600-400 p-2 text-sm">{errorMessage}</div>
  {/if}

  {#if isOwner}
    <!-- Owner-only tenant switcher for the members view below. Prominent so it
         is not mistaken for a minor control. -->
    <div
      class="flex flex-wrap items-center gap-3 rounded-container preset-outlined-primary-500 p-3 md:p-4"
    >
      <span class="fd fd-users text-[24px]"></span>
      <div class="flex flex-col">
        <span class="text-xs font-semibold tracking-wide uppercase opacity-70">
          View members of tenant
        </span>
        <span class="text-sm opacity-70">The users list below reflects this tenant.</span>
      </div>
      <Select class="ml-auto w-64" bind:value={adminTenant}>
        {#each $tenants as t (t.id)}
          <option value={t.id}>{t.name}</option>
        {/each}
      </Select>
    </div>
  {/if}

  {#snippet usersBody()}
    <UserRoleTable tenant={selectedTenant}></UserRoleTable>
  {/snippet}
  {@render section(
    `Users & roles — ${tenantLabel}`,
    'Members of this tenant and their roles. To manage this tenant’s OIDC trust relationships, use the "Manage OIDC trust" menu.',
    usersBody
  )}

  {#if isOwner}
    {#snippet ownerTrustBody()}
      <div class="scrollbar flex max-h-[40vh] flex-col gap-2 overflow-auto">
        {#each $ownerTrusts as trust (trust.id)}
          {#snippet deleteTrustDialog()}
            <GenericDialog
              content={{
                title: `Delete owner trust '${trust.name}'?`,
                description:
                  'Tokens matching this trust immediately lose platform-wide owner access. This cannot be undone.',
                onSuccess: {
                  name: 'Delete',
                  callback: async () => {
                    try {
                      await deleteOidcTrust(trust.name, undefined, true)
                      ownerTrusts.reload?.()
                    } catch (e) {
                      errorMessage = e instanceof Error ? e.message : String(e)
                    }
                    globalDialog.dialog = null
                  }
                },
                onCancel: {
                  callback: () => {
                    globalDialog.dialog = null
                  }
                }
              }}
              noclose
              danger
            ></GenericDialog>
          {/snippet}
          <div class="flex flex-nowrap items-center gap-2 border-b border-surface-100-900 py-2">
            <div class="w-full">
              <div>{trust.name}</div>
              <div class="text-sm opacity-70">
                <code>{trust.issuer}</code> · sub=<code>{trust.subject}</code>{#if trust.audience}
                  · aud=<code>{trust.audience}</code>{/if}
              </div>
              {#if trust.description}
                <div class="text-xs opacity-70">{trust.description}</div>
              {/if}
            </div>
            <button
              class="fd fd-trash-2 btn-icon text-[20px]"
              aria-label="Delete {trust.name} owner trust"
              onclick={() => (globalDialog.dialog = deleteTrustDialog)}
            ></button>
          </div>
        {:else}
          <div class="opacity-70">No owner trust relationships configured</div>
        {/each}
      </div>
      <NewOidcTrustForm fixedRole="owner" allowOwner={true} onSuccess={() => ownerTrusts.reload?.()}
      ></NewOidcTrustForm>
    {/snippet}
    {@render section(
      'Owner access (platform-wide OIDC trust)',
      'Owner trusts grant full platform access across all tenants and belong to no single tenant. This is the only place to manage them; a matching workload selects the tenant it acts in with the Feldera-Tenant header.',
      ownerTrustBody
    )}

    {#snippet tenantsBody()}
      <TenantList></TenantList>
    {/snippet}
    {@render section('Tenants', 'Owner-only: list and create tenants.', tenantsBody)}
  {/if}
</div>
