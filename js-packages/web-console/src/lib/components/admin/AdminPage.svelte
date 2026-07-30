<script lang="ts">
  import { Listbox, useListCollection } from '@skeletonlabs/skeleton-svelte'
  import { asyncReadable } from '@square/svelte-store'
  import { page } from '$app/state'
  import TenantList from '$lib/components/admin/TenantList.svelte'
  import UserRoleTable from '$lib/components/admin/UserRoleTable.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { usePermission } from '$lib/compositions/usePermission.svelte'
  import { getConfiguredOwners, getTenants, type Tenant } from '$lib/services/pipelineManager'
  import type { Snippet } from '$lib/types/svelte'

  // Tenant management (switch/list/create) is owner-only; gate on the permission
  // rather than the role directly, in line with the rest of the RBAC surface.
  const manageTenants = usePermission('write:tenant')
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
  // Tenants the header picker can switch to: every tenant except the one whose
  // members already show below, since the picker's job is to pick a different one.
  const tenantCollection = $derived(
    useListCollection({
      items: $tenants.filter((t) => t.id !== adminTenant),
      itemToValue: (t) => t.id,
      itemToString: (t) => t.name
    })
  )

  // Owner comes from deploy-time configuration, so this list is read-only: the
  // way to change it is to change the deployment. Owner-only, like the endpoint
  // behind it.
  const configuredOwners = asyncReadable<
    | {
        owners: string[]
        owner_trusts: { issuer: string; subject: string; audience?: string | null }[]
      }
    | undefined
  >(undefined, getConfiguredOwners)
</script>

{#snippet section(title: string | Snippet, description: string, body: Snippet)}
  <section class="flex flex-col gap-3 rounded-container border border-surface-200-800 p-4 md:p-6">
    <div>
      <h2 class="text-xl font-semibold">
        {#if typeof title === 'string'}{title}{:else}{@render title()}{/if}
      </h2>
      <p class="text-sm">{description}</p>
    </div>
    {@render body()}
  </section>
{/snippet}

<div class="mx-auto flex w-full max-w-4xl flex-col gap-6 px-2 pb-10 md:px-8">
  <h1 class="h2">Administration</h1>

  {#if errorMessage}
    <div class="rounded preset-outlined-error-600-400 p-2 text-sm">{errorMessage}</div>
  {/if}

  {#snippet usersBody()}
    <UserRoleTable tenant={selectedTenant}></UserRoleTable>
  {/snippet}
  {#snippet usersTitle()}
    Users &amp; roles for {#if manageTenants.allowed}
      <!-- Owner-only: click the tenant name to switch which tenant's members
           show below, without moving the global acting-tenant. -->
      <Popup wrapperClass="inline-block align-baseline">
        {#snippet trigger(toggle)}
          <button type="button" onclick={toggle} class="group inline-flex items-baseline gap-2">
            <span class="group-hover:underline">{tenantLabel}</span>

            <span class="text-base font-normal text-primary-500 group-hover:underline"
              >select a different tenant</span
            >
          </button>
        {/snippet}
        {#snippet content(close)}
          <div
            class="bg-white-dark absolute top-full -left-5 z-30 mt-1 max-h-64 w-64 overflow-auto rounded-container shadow-md"
          >
            <Listbox
              collection={tenantCollection}
              selectionMode="single"
              onValueChange={(e) => {
                adminTenant = e.value[0] ?? ''
                close()
              }}
            >
              <Listbox.Content class="bg-white-dark flex flex-col">
                {#each tenantCollection.items as t (t.id)}
                  <Listbox.Item
                    item={t}
                    class="cursor-pointer px-3 py-1.5 hover:preset-tonal-surface"
                  >
                    <Listbox.ItemText>{t.name}</Listbox.ItemText>
                  </Listbox.Item>
                {/each}
              </Listbox.Content>
            </Listbox>
          </div>
        {/snippet}
      </Popup>
    {:else}{tenantLabel}{/if}
  {/snippet}
  {@render section(
    usersTitle,
    'Members of this tenant and their roles. To manage this tenant’s OIDC trust relationships, use the "Manage OIDC trust" menu.',
    usersBody
  )}

  {#if manageTenants.allowed}
    {#snippet ownersBody()}
      <div class="flex flex-col gap-3">
        <div class="flex flex-col gap-1">
          <div class="text-lg font-medium">Users</div>
          {#each $configuredOwners?.owners ?? [] as owner (owner)}
            <div class=""><code>{owner}</code></div>
          {:else}
            <div class="">None configured</div>
          {/each}
        </div>
        <div class="flex flex-col gap-1">
          <div class="text-lg font-medium">Workloads (OIDC trust)</div>
          {#each $configuredOwners?.owner_trusts ?? [] as trust (trust.issuer + trust.subject)}
            <div class="">
              <code>{trust.issuer}</code> · sub=<code>{trust.subject}</code>{#if trust.audience}
                · aud=<code>{trust.audience}</code>{/if}
            </div>
          {:else}
            <div class="">None configured</div>
          {/each}
        </div>
      </div>
    {/snippet}
    {@render section(
      'Platform owners',
      'Owner is configured at deploy time (authorization.owners and authorization.ownerTrusts) and cannot be granted through the API, so this list is read-only.',
      ownersBody
    )}

    {#snippet tenantsBody()}
      <TenantList></TenantList>
    {/snippet}
    {@render section('Tenants', 'Owner-only: list and create tenants.', tenantsBody)}
  {/if}
</div>
