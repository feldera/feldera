<script lang="ts">
  import type { Snippet } from 'svelte'

  import ModalDrawer from '$lib/components/layout/ModalDrawer.svelte'
  import InlineDrawer from '$lib/components/layout/InlineDrawer.svelte'
  import { useIsMobile } from '$lib/compositions/layout/useIsMobile.svelte'
  const isMobile = useIsMobile()

  let {
    open = $bindable(),
    side,
    children,
    width
  }: {
    open: boolean
    side: 'right' | 'left' | 'top' | 'bottom'
    children: Snippet
    width: string
  } = $props()
</script>

<!-- {#if isMobile.matches} -->
{#if isMobile.current}
  <ModalDrawer {width} bind:open {side} {children} class="bg-surface-50 dark:bg-surface-950"
  ></ModalDrawer>
{:else}
  <InlineDrawer {width} {open} {side} {children}></InlineDrawer>
{/if}
