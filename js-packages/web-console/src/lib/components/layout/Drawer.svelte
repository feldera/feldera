<script lang="ts">
  import InlineDrawer from '$lib/components/layout/InlineDrawer.svelte'

  import OverlayDrawer from '$lib/components/layout/OverlayDrawer.svelte'
  import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'
  import type { Percent } from '$lib/functions/common/percent'
  import type { Snippet } from '$lib/types/svelte'

  const isTablet = useIsTablet()

  let {
    open = $bindable(),
    side,
    main,
    children,
    width = 'w-[700px]',
    onClose,
    inlineClass = '',
    defaultSize = '30%',
    minSize = '20%',
    maxSize = '80%',
    localStorageKey
  }: {
    open: boolean
    side: 'right' | 'left'
    /**
     * Always-visible content. On wide screens the drawer resizes against it
     * via a draggable divider; on tablet/mobile the drawer overlays it.
     */
    main: Snippet
    /** Drawer content. */
    children: Snippet
    /** Overlay-mode drawer width on tablet/mobile, e.g. `w-[700px]`. */
    width?: string
    inlineClass?: string
    /** Inline drawer pane size on open, as a percentage of the container. */
    defaultSize?: Percent
    minSize?: Percent
    maxSize?: Percent
    /** localStorage key to persist the resized inline layout. */
    localStorageKey?: string
    /**
     * Called when the drawer requests dismissal (currently fired by the
     * modal-backdrop click in the tablet/mobile overlay variant).
     */
    onClose?: () => void
  } = $props()
</script>

{#if isTablet.current}
  {@render main()}
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
  <InlineDrawer
    {open}
    {side}
    {main}
    {children}
    {defaultSize}
    {minSize}
    {maxSize}
    {localStorageKey}
    class="bg-white-dark {inlineClass}"
  ></InlineDrawer>
{/if}
