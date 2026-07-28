<script lang="ts">
  // Test-only harness: renders RBAC with a snippet child that surfaces `allowed`
  // and spreads `disabledProps`, so the spec can assert the wrapper's hide,
  // disable, and disabledProps behavior. Not used by the app.
  import RBAC from '$lib/components/auth/RBAC.svelte'
  import type { Permission } from '$lib/services/rbac'

  let { require: permission, mode = 'hide' }: { require: Permission; mode?: 'hide' | 'disable' } =
    $props()
</script>

<RBAC require={permission} {mode}>
  {#snippet children({ allowed, disabledProps })}
    <button data-testid="child" data-allowed={allowed} {...disabledProps}>child</button>
  {/snippet}
</RBAC>
