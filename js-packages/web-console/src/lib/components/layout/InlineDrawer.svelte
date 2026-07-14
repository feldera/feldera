<script lang="ts">
  import { Pane, PaneGroup, PaneResizer } from 'paneforge'
  import { type Percent, percentValue } from '$lib/functions/common/percent'
  import type { Snippet } from '$lib/types/svelte'

  const {
    side,
    open,
    main,
    children,
    defaultSize = '33%',
    minSize = '20%',
    maxSize = '80%',
    localStorageKey,
    class: _class = ''
  }: {
    /** Drawer open state */
    open: boolean
    /** Whether the drawer opens on the left or right of the container */
    side: 'right' | 'left'
    /** Always-visible content the drawer resizes against. */
    main: Snippet
    /** Drawer content, shown next to `main` when `open` is true. */
    children: Snippet
    /** Drawer pane size on open, as a percentage of the container width. */
    defaultSize?: Percent
    /** Smallest the drawer pane can be dragged, as a percentage of the container width. */
    minSize?: Percent
    /** Largest the drawer pane can be dragged, as a percentage of the container width. */
    maxSize?: Percent
    /** localStorage key to persist the resized layout across sessions. */
    localStorageKey?: string
    /** Classes of the drawer body wrapper */
    class?: string
  } = $props()

  const mainOrder = $derived(side === 'right' ? 1 : 2)
  const drawerOrder = $derived(side === 'right' ? 2 : 1)
  const mainMinSize = $derived(100 - percentValue(maxSize))
</script>

{#snippet mainPane()}
  <Pane order={mainOrder} minSize={mainMinSize} class="!overflow-visible">
    {@render main()}
  </Pane>
{/snippet}
{#snippet drawerPane()}
  <Pane
    order={drawerOrder}
    defaultSize={percentValue(defaultSize)}
    minSize={percentValue(minSize)}
    maxSize={percentValue(maxSize)}
    class="!overflow-visible"
  >
    <div class="h-full {_class}">
      {@render children()}
    </div>
  </Pane>
{/snippet}

<PaneGroup
  direction="horizontal"
  class="h-full min-w-0 flex-1 !overflow-visible"
  autoSaveId={localStorageKey}
>
  {#if open && side === 'left'}
    {@render drawerPane()}
    <PaneResizer class="pane-divider-vertical mx-2" />
  {/if}
  {@render mainPane()}
  {#if open && side === 'right'}
    <PaneResizer class="pane-divider-vertical mx-2" />
    {@render drawerPane()}
  {/if}
</PaneGroup>
