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
  const tenantLabel = $derived(
    adminTenant ? ($tenants.find((t) => t.id === adminTenant)?.name ?? adminTenant) : 'current tenant'
  )

  // Trusts for the selected tenant, split by grant scope: `owner` is a
  // platform-wide grant managed in its own section; read/write/admin are
  // tenant-scoped.
  const trusts = asyncReadable<OidcTrustDescr[]>([], () => getOidcTrustList(selectedTenant), {
    reloadable: true
  })
  const ownerTrusts = $derived($trusts.filter((t) => t.role === 'owner'))
  const tenantTrusts = $derived($trusts.filter((t) => t.role !== 'owner'))
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

{#snippet trustList(list: OidcTrustDescr[])}
  <div class="scrollbar flex max-h-[40vh] flex-col gap-2 overflow-auto">
    {#each list as trust (trust.id)}
      {#snippet deleteTrustDialog()}
        <GenericDialog
          content={{
            title: `Delete trust '${trust.name}'?`,
            description: 'Tokens matching this trust immediately lose access. This cannot be undone.',
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
      <div class="opacity-70">None configured</div>
    {/each}
  </div>
{/snippet}

<div class="mx-auto flex w-full max-w-4xl flex-col gap-6 px-2 pb-10 md:px-8">
  <h1 class="h2">Administration</h1>

  {#if isOwner}
    <!-- Owner-only tenant switcher. Prominent so it is not mistaken for a minor
         control: everything below (users, trusts) reflects the chosen tenant. -->
    <div
      class="flex flex-wrap items-center gap-3 rounded-container preset-outlined-primary-500 p-3 md:p-4"
    >
      <span class="fd fd-building text-[24px]"></span>
      <div class="flex flex-col">
        <span class="text-xs font-semibold uppercase tracking-wide opacity-70">
          Viewing &amp; managing tenant
        </span>
        <span class="text-sm opacity-70">Everything below applies to this tenant.</span>
      </div>
      <Select class="ml-auto w-64" bind:value={adminTenant}>
        <option value="">Current tenant</option>
        {#each $tenants as t (t.id)}
          <option value={t.id}>{t.name}</option>
        {/each}
      </Select>
    </div>
  {/if}

  {#if errorMessage}
    <div class="rounded preset-outlined-error-600-400 p-2 text-sm">{errorMessage}</div>
  {/if}

  {#snippet usersBody()}
    <UserRoleTable tenant={selectedTenant}></UserRoleTable>
  {/snippet}
  {@render section(
    `Users & roles — ${tenantLabel}`,
    'Members of this tenant and their roles.',
    usersBody
  )}

  {#snippet tenantTrustBody()}
    {@render trustList(tenantTrusts)}
    <NewOidcTrustForm allowOwner={false} tenant={selectedTenant} onSuccess={() => trusts.reload?.()}
    ></NewOidcTrustForm>
  {/snippet}
  {@render section(
    `Tenant OIDC trust — ${tenantLabel}`,
    'Grant read/write/admin to workloads (CI, services) in this tenant by trusting JWTs from an issuer.',
    tenantTrustBody
  )}

  {#if isOwner}
    {#snippet ownerTrustBody()}
      {@render trustList(ownerTrusts)}
      <NewOidcTrustForm
        fixedRole="owner"
        allowOwner={true}
        tenant={selectedTenant}
        onSuccess={() => trusts.reload?.()}
      ></NewOidcTrustForm>
    {/snippet}
    {@render section(
      'Owner access (platform-wide)',
      'Owner trusts grant full platform access across all tenants. Only owners manage these, and only here.',
      ownerTrustBody
    )}

    {#snippet tenantsBody()}
      <TenantList></TenantList>
    {/snippet}
    {@render section('Tenants', 'Owner-only: list and create tenants.', tenantsBody)}
  {/if}
</div>
