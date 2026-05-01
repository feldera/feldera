<script lang="ts">
  import InlineDrawer from '$lib/components/layout/InlineDrawer.svelte'

  import OverlayDrawer from '$lib/components/layout/OverlayDrawer.svelte'
  import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'
  import type { Snippet } from '$lib/types/svelte'

  const isTablet = useIsTablet()

  let {
    open = $bindable(),
    side,
    children,
    width,
    onClose,
    inlineClass = ''
  }: {
    open: boolean
    side: 'right' | 'left'
    children: Snippet
    width: string
    inlineClass?: string
    /**
     * Called when the drawer requests dismissal (currently fired by the
     * modal-backdrop click in the tablet/mobile overlay variant).
     */
    onClose?: () => void
  } = $props()
</script>

<!-- {#if isTablet.matches} -->
{#if isTablet.current}
  <OverlayDrawer
    {width}
    bind:open
    {side}
    {children}
    {onClose}
    modal={true}
    class="bg-white-dark p-4"
  ></OverlayDrawer>
{:else}
  <InlineDrawer {width} {open} {side} {children} class="bg-white-dark {inlineClass}"></InlineDrawer>
{/if}
