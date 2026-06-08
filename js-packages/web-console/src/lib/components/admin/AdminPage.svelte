<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { page } from '$app/state'
  import TenantList from '$lib/components/admin/TenantList.svelte'
  import UserRoleTable from '$lib/components/admin/UserRoleTable.svelte'
  import NewOidcTrustForm from '$lib/components/oidcTrust/NewOidcTrustForm.svelte'
  import {
    deleteOidcTrust,
    getOidcTrustList,
    type OidcTrustDescr
  } from '$lib/services/pipelineManager'
  import type { Snippet } from '$lib/types/svelte'

  const isOwner = $derived(page.data.feldera?.isOwner ?? false)

  // OIDC trust list reused inline (admin/owner are granted to non-human
  // principals through trust relationships).
  const trusts = asyncReadable<OidcTrustDescr[]>([], getOidcTrustList, { reloadable: true })
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

  {#snippet usersBody()}
    <UserRoleTable></UserRoleTable>
  {/snippet}
  {@render section(
    'Users & roles',
    'Manage tenant members and their roles.',
    usersBody
  )}

  {#snippet oidcBody()}
    <div class="scrollbar flex max-h-[40vh] flex-col gap-2 overflow-auto">
      {#each $trusts as trust (trust.id)}
        <div class="flex flex-nowrap items-center gap-2 border-b border-surface-100-900 py-2">
          <div class="w-full">
            <div>
              {trust.name}
              <span class="text-xs opacity-70">[{trust.role}]</span>
            </div>
            <div class="text-sm opacity-70">
              <code>{trust.issuer}</code> · sub=<code>{trust.subject}</code
              >{#if trust.audience}
                · aud=<code>{trust.audience}</code>{/if}
            </div>
            {#if trust.description}
              <div class="text-xs opacity-70">{trust.description}</div>
            {/if}
          </div>
          <button
            class="fd fd-trash-2 btn-icon text-[20px]"
            aria-label="Delete {trust.name} trust relationship"
            onclick={async () => {
              await deleteOidcTrust(trust.name)
              trusts.reload?.()
            }}
          ></button>
        </div>
      {:else}
        <div class="opacity-70">No OIDC trust relationships configured</div>
      {/each}
    </div>
    <NewOidcTrustForm onSuccess={() => trusts.reload?.()}></NewOidcTrustForm>
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
