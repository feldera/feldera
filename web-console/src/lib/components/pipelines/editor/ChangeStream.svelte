<script lang="ts" context="module">
</script>

<script lang="ts">
  import type { XgressRecord } from '$lib/types/pipelineManager'
  import JSONbig from 'true-json-bigint'

  import { VList } from 'virtua/svelte'

  let {
    changes
  }: {
    changes: ({ relationName: string } & ({ insert: XgressRecord } | { delete: XgressRecord }))[]
  } = $props()

  let ref: VList<any>
  let len = $derived(changes.length)
  let lastScrollOffset = $state(0) // NOTE: Part of a workaround, see below
  {
    // Keep scroll position at the bottom of the list when its length increases if it's there already
    let lastScrollSize = $state(0)
    $effect(() => {
      len
      if (!ref) {
        return
      }
      if (lastScrollSize === ref.getScrollSize()) {
        return
      }
      const curScroll = Math.round(ref.getScrollOffset() + ref.getViewportSize())
      if (curScroll === lastScrollSize) {
        ref.scrollTo(ref.getScrollSize())
        lastScrollOffset = ref.getScrollOffset()
      } else if (lastScrollSize > curScroll && lastScrollOffset === 0) {
        // NOTE: There is a bug with incorrectly reporting scroll position in a virtual list.
        // When the virtual list is scrolled all the way to the bottom,
        // the first time list length is increased after the list is already longer than the visible height,
        // the following does not hold (but it should):
        // Math.round(ref.getScrollOffset()) + Math.round(ref.getViewportSize()) === lastScrollSize
        // We need this special case and to update `lastScrollOffset` as a hack to fix this behavior
        ref.scrollTo(ref.getScrollSize())
        lastScrollOffset = ref.getScrollOffset()
      }
      lastScrollSize = Math.round(ref.getScrollSize())
    })
  }
  $effect(() => {
    if (!ref) {
      return
    }
    // Make sure to scroll to beginning when jumping from list with some items to none
    if (len === 0) {
      ref.scrollTo(0)
    }
  })
</script>

<div class="flex-1">
  <VList data={changes} let:item getKey={(d, i) => i} bind:this={ref}>
    <div
      class={`even:bg-surface-100-900 whitespace-nowrap pl-2 before:inline-block before:w-2 even:!bg-opacity-30 ` +
        ('insert' in item
          ? "shadow-[inset_26px_0px_0px_0px_rgba(0,255,0,0.3)] before:content-['+']"
          : 'delete' in item
            ? "shadow-[inset_26px_0px_0px_0px_rgba(255,0,0,0.3)] before:pl-[1px] before:content-['-']"
            : '')}>
      <span class="inline-block w-64 overflow-clip overflow-ellipsis pl-4"
        >{item.relationName}</span>
      <span class="">{JSONbig.stringify((item as any).insert ?? (item as any).delete)}</span>
    </div>
  </VList>
</div>
