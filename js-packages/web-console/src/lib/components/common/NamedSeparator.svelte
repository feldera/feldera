<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  let { children, class: _class = '' }: { children: Snippet; class?: string } = $props()
  // Example classes: text-surface-500 before:border-surface-500 after:border-surface-500
</script>

<div
  class={'flex items-center py-2 text-sm uppercase before:me-6 before:flex-1 before:border-t after:ms-6 after:flex-1 after:border-t ' +
    _class}
>
  {@render children()}
</div>
