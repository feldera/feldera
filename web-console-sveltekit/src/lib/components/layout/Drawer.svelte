<script lang="ts">
  import type { Snippet } from 'svelte'

  import ModalDrawer from '$lib/components/layout/ModalDrawer.svelte'
  import { listen } from 'svelte-mq-store'
  import { MediaQuery } from 'runed'
  import InlineDrawer from '$lib/components/layout/InlineDrawer.svelte'
  const isMobile = listen('(max-width: 1200px)')
  // const isMobile = new MediaQuery('(max-width: 1200px)')

  let {
    open = $bindable(),
    side,
    children
  }: {
    open: boolean
    side: 'right' | 'left' | 'top' | 'bottom'
    children: Snippet
  } = $props()
</script>

<!-- {#if isMobile.matches} -->
{#if $isMobile}
  <ModalDrawer bind:open {side} {children} class="bg-surface-50 dark:bg-surface-950"></ModalDrawer>
{:else}
  <InlineDrawer {open} {side} {children}></InlineDrawer>
{/if}
