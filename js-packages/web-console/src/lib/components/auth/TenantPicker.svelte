<script lang="ts">
  import { switchTenant } from '$lib/compositions/switchTenant'
  import type { TenantMembership } from '$lib/types/auth'

  const { memberships }: { memberships: TenantMembership[] } = $props()

  // Disable the list after the first click: selecting restarts the app, and a
  // second click meanwhile would race the reload with another selection.
  let selecting = $state(false)

  // Select by tenant id (rename-stable, and unambiguous even when a tenant is
  // named after a UUID-shaped OIDC sub). Reload in place so a deep link that
  // hit this gate lands on the page the user asked for.
  const select = (membership: TenantMembership) => {
    selecting = true
    switchTenant(membership.tenantId, { reloadInPlace: true })
  }
</script>

<div class="mx-auto flex w-full max-w-md flex-col gap-4 p-8">
  {#if memberships.length > 0}
    <div>
      <h1 class="h3">Choose a tenant</h1>
      <p class="text-sm text-surface-800-200">
        Your account is a member of several tenants. Pick the one to work in; you can switch later
        from the tenant menu in the header.
      </p>
    </div>
    <div class="flex flex-col gap-2">
      {#each memberships as membership (membership.tenantId)}
        <button
          class="btn flex justify-between preset-outlined-surface-200-800 hover:preset-tonal-surface"
          disabled={selecting}
          onclick={() => select(membership)}
        >
          <span class="font-medium">{membership.name}</span>
          <span class="text-sm text-surface-800-200">{membership.role}</span>
        </button>
      {/each}
    </div>
  {:else}
    <div>
      <h1 class="h3">No tenant access</h1>
      <p class="text-sm text-surface-800-200">
        You have no tenant access yet. Ask an administrator to add you to a tenant, then reload this
        page.
      </p>
    </div>
  {/if}
</div>
