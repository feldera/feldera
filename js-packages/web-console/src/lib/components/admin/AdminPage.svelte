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

  // Owner-only per-tenant view: pick a tenant (by UUID) to inspect and manage
  // its members and trusts in place, without changing the global acting-tenant.
  // Empty string means the owner's own (globally selected) tenant.
  let adminTenant = $state('')
  const selectedTenant = $derived(adminTenant || undefined)
  const tenants = asyncReadable<Tenant[]>([], getTenants, { reloadable: true })

  // OIDC trust list reused inline (admin/owner are granted to non-human
  // principals through trust relationships). Re-fetched for the selected tenant.
  const trusts = asyncReadable<OidcTrustDescr[]>([], () => getOidcTrustList(selectedTenant), {
    reloadable: true
  })
  $effect(() => {
    selectedTenant
    trusts.reload?.()
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
  <div class="flex flex-wrap items-center justify-between gap-3">
    <h1 class="h2">Administration</h1>
    {#if isOwner}
      <label class="flex items-center gap-2 text-sm">
        <span class="opacity-70">Managing tenant</span>
        <Select class="w-56" bind:value={adminTenant}>
          <option value="">My tenant</option>
          {#each $tenants as t (t.id)}
            <option value={t.id}>{t.name}</option>
          {/each}
        </Select>
      </label>
    {/if}
  </div>

  {#if errorMessage}
    <div class="rounded preset-outlined-error-600-400 p-2 text-sm">{errorMessage}</div>
  {/if}

  {#snippet usersBody()}
    <UserRoleTable tenant={selectedTenant}></UserRoleTable>
  {/snippet}
  {@render section('Users & roles', 'Manage tenant members and their roles.', usersBody)}

  {#snippet oidcBody()}
    <div class="scrollbar flex max-h-[40vh] flex-col gap-2 overflow-auto">
      {#each $trusts as trust (trust.id)}
        {#snippet deleteTrustDialog()}
          <GenericDialog
            content={{
              title: `Delete trust '${trust.name}'?`,
              description:
                'Tokens matching this trust immediately lose access. This cannot be undone.',
              onSuccess: {
                name: 'Delete',
                callback: async () => {
                  try {
                    await deleteOidcTrust(trust.name, selectedTenant)
                    trusts.reload?.()
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
            <div>
              {trust.name}
              <span class="text-xs opacity-70">[{trust.role}]</span>
            </div>
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
            aria-label="Delete {trust.name} trust relationship"
            onclick={() => (globalDialog.dialog = deleteTrustDialog)}
          ></button>
        </div>
      {:else}
        <div class="opacity-70">No OIDC trust relationships configured</div>
      {/each}
    </div>
    <NewOidcTrustForm
      allowOwner={isOwner}
      tenant={selectedTenant}
      onSuccess={() => trusts.reload?.()}
    ></NewOidcTrustForm>
  {/snippet}
  {@render section(
    'Admin & owner access (OIDC trust)',
    'Grant roles to non-human principals (CI, services) by trusting JWTs from an issuer.',
    oidcBody
  )}

  {#if isOwner}
    {#snippet tenantsBody()}
      <TenantList></TenantList>
    {/snippet}
    {@render section(
      'Tenants',
      'Owner-only: list and create tenants, and switch the active tenant.',
      tenantsBody
    )}
  {/if}
</div>
