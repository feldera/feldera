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
    itemCount,
    itemSize,
    listContainer = defaultListContainer,
    header,
    footer,
    stickyIndices = [],
    onscroll: _onscroll
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
    listContainer?: Snippet<[Snippet, ListContainer]>
    placeholder?: Snippet<[Item]>
    footer?: Snippet
  } = $props()

  export function scrollToBottom() {
    if (!setref) {
      return
    }
    setref.scrollTo({ top: setref.scrollHeight })
  }

  let scrollTop = $state(0)
  let clientHeight = $state(0)
  const overscan = 1
  let indexOffset = $derived(Math.max(Math.round(scrollTop / itemSize) - overscan, 0))
  let visibleCount = $derived(Math.round(clientHeight / itemSize) + overscan)
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
  let setref = $state<Element>()
</script>

{#snippet defaultListContainer(
  children: Snippet,
  { width, height }: { height: string; width: string }
)}
  <div style:width style:height>
    {@render children()}
  </div>
{/snippet}

{@render listContainer(listBody, {
  height: `${itemCount * itemSize}px`,
  width: '100%',
  onscroll,
  onresize,
  setref: (v) => (setref = v)
})}

{#snippet listBody()}
  {@render header?.()}
  {#if indexOffset % 2 == 0}
    <!-- Preserve even-odd coloring of elements -->
    <div></div>
  {/if}
  {#if stickyRow !== undefined}
    {@render item({
      index: stickyRow,
      padding: '',
      isSticky: true
    })}
  {/if}
  {#each indices as index, i}
    {@render item({
      index,
      style: `transform: translateY(${(indices[0] - (stickyRow === undefined ? 0 : 1)) * itemSize}px);`
    })}
  {/each}
  {@render footer?.()}
{/snippet}
