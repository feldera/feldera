<script lang="ts" context="module">
</script>

<script lang="ts">
  import type { XgressRecord } from '$lib/types/pipelineManager'
  import JSONbig from 'true-json-bigint'

  import { VirtualList, type AfterScrollEvent } from 'svelte-virtuallists'
  import { useResizeObserver } from 'runed'
  import { scale } from 'svelte/transition'

  let {
    changes
  }: {
    changes: ({ relationName: string } & ({ insert: XgressRecord } | { delete: XgressRecord }))[]
  } = $props()

  let len = $derived(changes.length)
  let lastLen = $state(changes.length)
  let scrollOffset = $state(0)
  let lastScrollOffset = $state(0)
  const itemSize = 24
  let ref = $state<HTMLElement>()
  let height = $state(0)
  let didFirstScroll = $state(false)
  const onAfterScroll = (e: AfterScrollEvent) => {
    lastScrollOffset = Number(e.offset)
  }
  useResizeObserver(
    () => ref,
    (entries) => {
      const entry = entries[0]
      if (!entry) {
        return
      }
      height = entry.contentRect.height
    }
  )
  $effect(() => {
    if (height === 0) {
      return
    }
    if (lastLen === len && didFirstScroll) {
      return
    }
    stickToBottom(lastLen, len)
    lastLen = len
  })
  const stickToBottom = (lastLen: number, len: number) => {
    if (lastScrollOffset !== 0 && Math.round(lastScrollOffset + height) >= lastLen * itemSize) {
      // Scroll to the new bottom of the list if scroll was at the bottom previously
      lastScrollOffset = scrollOffset = len * itemSize
    } else if (!didFirstScroll && len > height / itemSize) {
      // Scroll to the bottom of the list the first time it became longer than the viewport
      lastScrollOffset = scrollOffset = len * itemSize
      didFirstScroll = true
    }
  }
</script>

<div class="relative flex-1" bind:this={ref}>
  <VirtualList
    width="100%"
    {height}
    model={changes}
    {scrollOffset}
    modelCount={changes.length}
    {itemSize}
    {onAfterScroll}
  >
    {#snippet slot({ item, style })}
      <div
        {style}
        class={`row whitespace-nowrap pl-2 before:inline-block before:w-2 even:!bg-opacity-30 even:bg-surface-100-900 ` +
          ('insert' in item
            ? "shadow-[inset_26px_0px_0px_0px_rgba(0,255,0,0.3)] before:content-['+']"
            : 'delete' in item
              ? "shadow-[inset_26px_0px_0px_0px_rgba(255,0,0,0.3)] before:pl-[1px] before:content-['-']"
              : '')}
      >
        <span class="inline-block w-64 overflow-clip overflow-ellipsis pl-4"
          >{item.relationName}</span
        >
        <span class="">{JSONbig.stringify((item as any).insert ?? (item as any).delete)}</span>
      </div>
    {/snippet}
  </VirtualList>
  {#if height !== 0 && Math.round(lastScrollOffset + height) < len * itemSize}
    <button
      transition:scale={{ duration: 200 }}
      class="bx bx-chevrons-down absolute bottom-4 right-4 rounded-full p-2 text-[24px] preset-filled-primary-500"
      onclick={() => {
        // Force scroll to bottom
        scrollOffset = undefined!
        setTimeout(() => (scrollOffset = len * itemSize))
      }}
    ></button>
  {/if}
</div>
