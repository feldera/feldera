<script lang="ts" module>
  export type Item = {
    index: number
    style?: string
    padding?: string
    isSticky?: boolean
  }
  export type ListContainer = {
    height: string
    width: string
    onscroll: (event: ScrollEvent) => void
    setClientHeight: (value: number) => void
  }
</script>

<script lang="ts">
  import type { OnScroll, ScrollEvent } from './types'
  import type { Snippet } from '$lib/types/svelte'
  import { binarySearchMax } from '$lib/functions/common/array'
  let {
    item,
    emptyItem,
    itemCount,
    itemSize,
    listContainer = defaultListContainer,
    header,
    footer,
    stickyIndices = [],
    onscroll: _onscroll,
    overScan = 0,
    marginTop = 0,
    children
  }: {
    itemCount: number
    itemSize: number
    stickyIndices?: number[]
    /**
     * @default 1
     */
    overScan?: number
    /**
     * @default
     * ```ts
     * (index: number) => index
     * ```
     */
    onscroll?: OnScroll
    header?: Snippet
    item: Snippet<[Item]>
    /**
     * An empty item is rendered to preserve even-odd coloring of list items
     */
    emptyItem: Snippet
    listContainer?: Snippet<[Snippet, ListContainer, Snippet | undefined]>
    placeholder?: Snippet<[Item]>
    footer?: Snippet
    marginTop?: number
    children?: Snippet
  } = $props()

  let scrollTop = $state(0)
  let clientHeight = $state(0)
  let indexOffset = $derived(Math.max(Math.round(scrollTop / itemSize) - overScan - 1, 0))
  let visibleCount = $derived(Math.round((clientHeight - marginTop) / itemSize) + 2 + 2 * overScan)

  let stickyRow = $derived(
    ((i) => (i === -1 ? undefined : stickyIndices[i]))(
      binarySearchMax(stickyIndices, indexOffset + 1)
    )
  )
  // `visibleCount` rows exactly cover the viewport in the worst rounding case: `indexOffset` may
  // start half a row above it and the count may round half a row down, and the two spare rows in
  // `visibleCount` pay for both. Rendering one fewer left a strip up to a row tall blank along the
  // bottom edge at some offsets, which is what used to read as the list flickering while scrolling.
  let indices = $derived(Array.from({ length: visibleCount }, (_, i) => i + indexOffset))

  const onscroll = (event: ScrollEvent) => {
    scrollTop = event.currentTarget.scrollTop
    _onscroll?.(event)
  }
</script>

{#snippet defaultListContainer(
  items: Snippet,
  { width, height }: { height: string; width: string }
)}
  <div style:width style:height>
    {@render items()}
    {@render children?.()}
  </div>
{/snippet}

{@render listContainer(
  listBody,
  {
    height: `${itemCount * itemSize + marginTop}px`,
    width: '100%',
    onscroll,
    setClientHeight(value: number) {
      clientHeight = value
    }
  },
  children
)}

{#snippet listBody()}
  {@render header?.()}
  {#if indexOffset % 2 == 0}
    <!-- Preserve even-odd coloring of elements -->
    {@render emptyItem()}
  {/if}
  {#if stickyRow !== undefined}
    {@render item({
      index: stickyRow,
      padding: '',
      isSticky: true
    })}
  {/if}
  {#each indices as index, i (index)}
    {@render item({
      index,
      style: `transform: translateY(${(indexOffset - (stickyRow === undefined ? 0 : 1)) * itemSize}px); white-space: nowrap;`
    })}
  {/each}
  {@render footer?.()}
{/snippet}
