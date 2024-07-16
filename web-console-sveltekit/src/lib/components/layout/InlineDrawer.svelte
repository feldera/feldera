<script lang="ts">
  import type { Snippet } from 'svelte'
  let { open, children } = $props<{
    open: boolean
    side: 'right' | 'left' | 'top' | 'bottom'
    children: Snippet
  }>()
</script>

<div class={open ? 'w-72 duration-300 ease-in-out' : 'flex w-0 duration-300 ease-in-out'}>
  <div
    class={'transition-transform ' +
      (open
        ? 'visible translate-x-0 delay-100 duration-200 ease-in-out'
        : 'invisible -translate-x-full duration-300 ease-in-out')}
  >
    {@render children()}
  </div>
</div>
