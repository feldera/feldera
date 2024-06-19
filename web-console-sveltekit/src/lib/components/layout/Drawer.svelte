<script lang="ts">
  import type { Snippet } from 'svelte'

  import ModalDrawer from '$lib/components/layout/ModalDrawer.svelte'
  import { listen } from 'svelte-mq-store'
  import InlineDrawer from '$lib/components/layout/InlineDrawer.svelte'
  const isMobile = listen('(max-width: 1200px)')

  let {
    open = $bindable(),
    side,
    children
  } = $props<{
    open: boolean
    side: 'right' | 'left' | 'top' | 'bottom'
    children: Snippet
  }>()
</script>

{#if $isMobile}
  <ModalDrawer bind:open {side} class="bg-surface-50 dark:bg-surface-950" {children}></ModalDrawer>
{:else}
  <InlineDrawer {open} {side} {children}></InlineDrawer>
{/if}
