<script lang="ts" module>
  export type RBACState = {
    allowed: boolean
    // Spread onto the gated element to apply the read-only look when disallowed;
    // empty object when allowed.
    disabledProps: Record<string, unknown>
  }
</script>

<script lang="ts">
  import type { Snippet } from 'svelte'
  import { page } from '$app/state'
  import { hasPermissions, type Permission } from '$lib/services/rbac'

  let {
    require: permission,
    mode = 'hide',
    message = 'You have read-only access',
    children
  }: {
    require: Permission
    mode?: 'hide' | 'disable'
    message?: string
    children: Snippet<[RBACState]>
  } = $props()

  const allowed = $derived(hasPermissions(page.data.feldera, permission))
  const state = $derived<RBACState>({
    allowed,
    disabledProps: allowed
      ? {}
      : {
          disabled: true,
          'aria-disabled': 'true',
          title: message,
          class: 'pointer-events-none opacity-50'
        }
  })
</script>

{#if mode === 'disable' || allowed}
  {@render children(state)}
{/if}
