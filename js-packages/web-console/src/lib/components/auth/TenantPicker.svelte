<script lang="ts">
  import { switchTenant } from '$lib/compositions/switchTenant'
  import { takeRedirectTarget } from '$lib/services/redirectTarget'
  import type { TenantMembership } from '$lib/types/auth'

  const { memberships }: { memberships: TenantMembership[] } = $props()

  // Disable the list after the first click: selecting restarts the app, and a
  // second click meanwhile would race the reload with another selection.
  let selecting = $state(false)

  // Select by tenant id (rename-stable, and unambiguous even when a tenant is
  // named after a UUID-shaped OIDC sub). Restart on the page the gate
  // interrupted, so a deep link survives the detour through this page.
  const select = (membership: TenantMembership) => {
    selecting = true
    switchTenant(membership.tenantId, { to: takeRedirectTarget() })
  }
</script>

<div class="mx-auto flex w-full max-w-md flex-col gap-4 p-8">
  {#if memberships.length > 0}
    <h2 class="h2">Choose a tenant</h2>
  {:else}
    <h2 class="h2">No tenant access</h2>
  {/if}
  <div class="flex flex-col gap-4 rounded-container border border-surface-200-800 p-4 md:p-6">
    {#if memberships.length > 0}
      <p class="text-lg">
        Your account is a member of several tenants.<br />Pick the one to work in; you can switch
        later from the profile popup menu in the top right.
      </p>
      <div class="flex flex-col gap-2">
        {#each memberships as membership (membership.tenantId)}
          <button
            class="btn flex justify-between preset-outlined-surface-200-800 hover:preset-tonal-surface"
            disabled={selecting}
            onclick={() => select(membership)}
          >
            <span class="font-medium">{membership.name}</span>
            <span class="">{membership.role}</span>
          </button>
        {/each}
      </div>
    {:else}
      <p class="text-lg">
        You have no tenant access yet. Ask an administrator to add you to a tenant, then reload this
        page.
      </p>
    {/if}
  </div>
</div>
