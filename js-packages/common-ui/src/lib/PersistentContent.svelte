<!--
  Renders content once, at the root of the layout, in a fixed-position overlay
  that tracks the position and size of a placeholder elsewhere in the tree.

  Use it for content that is expensive to (re-)initialise and must survive being
  conditionally re-rendered — e.g. the profile viewer's dataflow diagram, whose
  underlying graph library would otherwise lose its state every time the layout
  toggles between the two panel arrangements. Instead of mounting the content
  inside the conditional, mount it here once and drop a `<div use:handle.placeholder>`
  wherever it should *appear*; this overlay follows that rect.

  Pair with `usePersistentRect()`: pass its handle as `persistent` here, and
  apply `handle.placeholder` to the placeholder div. See SupportBundleViewerLayout.svelte.
-->
<script lang="ts" module>
  import type { Snippet } from 'svelte'
  import type { PersistentHandle } from './persistentRect.svelte'

  export type PersistentContentProps = {
    /** Handle from `usePersistentRect()`. Pair this with the `placeholder` action
     *  applied to a `<div use:handle.placeholder>` elsewhere in the layout. */
    persistent: PersistentHandle
    /** Extra classes on the outer fixed-position wrapper (background, rounded corners,
     *  overflow clipping). Don't override `position: fixed` from here. */
    class?: string
    children: Snippet
  }
</script>

<script lang="ts">
  let { persistent, class: className = '', children }: PersistentContentProps = $props()
</script>

<!-- Always-mounted overlay. The fixed positioning is what lets it visually overlap the
     placeholder's rect regardless of which conditional branch the placeholder is in.
     `visibility: hidden` while the placeholder is unmounted or zero-sized; that still
     preserves layout for any inner library (Cytoscape, etc.) measuring its own size. -->
<div
  class="fixed {className}"
  style:left="{persistent.rect.left}px"
  style:top="{persistent.rect.top}px"
  style:width="{persistent.rect.width}px"
  style:height="{persistent.rect.height}px"
  style:visibility={persistent.rect.visible ? 'visible' : 'hidden'}
>
  {@render children()}
</div>
