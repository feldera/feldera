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
    onresize: (event: { clientHeight: number }) => void
    setref: (setref: HTMLElement) => void
  }
</script>

<script lang="ts">
  import type { GetKey, OnScroll, ScrollBehavior, ScrollEvent } from './types'
  import type { Snippet } from 'svelte'
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

  export function scrollToBottom() {
    if (!containerRef) {
      return
    }
    containerRef.scrollTo({ top: containerRef.scrollHeight })
  }

  let scrollTop = $state(0)
  let clientHeight = $state(0)
  let indexOffset = $derived(
    Math.max(Math.round((scrollTop - marginTop) / itemSize) - overScan - 1, 0)
  )
  let visibleCount = $derived(Math.round((clientHeight - marginTop) / itemSize) + 2 + 2 * overScan)

  let stickyRow = $derived(
    ((i) => (i === -1 ? undefined : stickyIndices[i]))(
      binarySearchMax(stickyIndices, indexOffset + 1)
    )
  )
  let indices = $derived(Array.from({ length: visibleCount }, (_, i) => i + indexOffset))

  const onscroll = (event: ScrollEvent) => {
    scrollTop = event.currentTarget.scrollTop
    _onscroll?.(event)
  }

  const onresize = (event: { clientHeight: number }) => {
    clientHeight = event.clientHeight
  }
  let containerRef = $state<Element>()
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
    onresize,
    setref: (v) => (containerRef = v)
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
      style: `transform: translateY(${(indexOffset - (stickyRow === undefined ? 0 : 1)) * itemSize}px);`
    })}
  {/each}
  {@render footer?.()}
{/snippet}
