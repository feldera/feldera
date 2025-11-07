<script lang="ts">
  import type { Snippet } from 'svelte'

  import OverlayDrawer from '$lib/components/layout/OverlayDrawer.svelte'
  import InlineDrawer from '$lib/components/layout/InlineDrawer.svelte'
  import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'
  const isTablet = useIsTablet()

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

<!-- {#if isTablet.matches} -->
{#if isTablet.current}
  <OverlayDrawer
    {width}
    bind:open
    {side}
    {children}
    modal={true}
    class="bg-surface-50 dark:bg-surface-950"
  ></OverlayDrawer>
{:else}
  <InlineDrawer {width} {open} {side} {children}></InlineDrawer>
{/if}
