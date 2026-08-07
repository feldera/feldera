<script lang="ts">
  import { Select } from 'common-ui'
  import { page } from '$app/state'
  import { switchTenant } from '$lib/compositions/switchTenant'

  let { class: className = '' }: { class?: string } = $props()

  const feldera = $derived(page.data.feldera)
  // Owners can act in a tenant outside their membership list (the admin page's
  // act-as flow); the acting tenant heads the list so it displays and remains
  // selectable, even though it is no membership.
  const memberships = $derived.by(() => {
    const granted = feldera?.memberships ?? []
    if (feldera === undefined || granted.some((m) => m.tenantId === feldera.tenantId)) {
      return granted
    }
    return [
      { tenantId: feldera.tenantId, name: feldera.tenantName, role: feldera.role },
      ...granted
    ]
  })
</script>

<!-- The acting tenant name is always shown; the dropdown affordance only when
     there is more than one tenant to act in.
     Switching selects by tenant id: ids are rename-stable, and the
     Feldera-Tenant resolver matches a UUID-shaped selector by id first, while
     per-sub personal tenants are named after the OIDC sub (itself a UUID on
     AWS Cognito), so a name-based selection could resolve a different tenant
     than the one clicked. -->
{#if feldera && (memberships.length > 1 || feldera.tenantName)}
  <label class="label {className}">
    <span class="text-left">Tenant</span>
    {#if memberships.length > 1}
      <Select
        value={feldera.tenantId}
        onchange={(e) => switchTenant(e.currentTarget.value)}
        class="text-base"
      >
        {#each memberships as membership (membership.tenantId)}
          <option value={membership.tenantId}>{membership.name}</option>
        {/each}
      </Select>
    {:else}
      <span class="text-base">{feldera.tenantName}</span>
    {/if}
  </label>
{/if}
