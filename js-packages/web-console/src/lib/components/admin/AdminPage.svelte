<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { Select } from 'common-ui'
  import { page } from '$app/state'
  import TenantList from '$lib/components/admin/TenantList.svelte'
  import UserRoleTable from '$lib/components/admin/UserRoleTable.svelte'
  import { getConfiguredOwners, getTenants, type Tenant } from '$lib/services/pipelineManager'
  import type { Snippet } from '$lib/types/svelte'

  const isOwner = $derived(page.data.feldera?.isOwner ?? false)
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
    {#snippet ownersBody()}
      <div class="flex flex-col gap-3">
        <div class="flex flex-col gap-1">
          <div class="text-sm font-medium">Users</div>
          {#each $configuredOwners?.owners ?? [] as owner (owner)}
            <div class="text-sm"><code>{owner}</code></div>
          {:else}
            <div class="text-sm opacity-70">None configured</div>
          {/each}
        </div>
        <div class="flex flex-col gap-1">
          <div class="text-sm font-medium">Workloads (OIDC trust)</div>
          {#each $configuredOwners?.owner_trusts ?? [] as trust (trust.issuer + trust.subject)}
            <div class="text-sm">
              <code>{trust.issuer}</code> · sub=<code>{trust.subject}</code>{#if trust.audience}
                · aud=<code>{trust.audience}</code>{/if}
            </div>
          {:else}
            <div class="text-sm opacity-70">None configured</div>
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
