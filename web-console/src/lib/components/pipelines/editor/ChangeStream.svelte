<script lang="ts" context="module">
</script>

<script lang="ts">
  import { accumulateChanges } from '$lib/functions/pipelines/changeStream'

  import { relationEggressStream } from '$lib/services/pipelineManager'
  import type { XgressRecord } from '$lib/types/pipelineManager'
  import type BigNumber from 'bignumber.js'
  import JSONbig from 'true-json-bigint'

  import { VList } from 'virtua/svelte'

  let {
    changes
  }: {
    // changes: { relationName: string; type: 'insert' | 'delete'; record: XgressRecord }[]
    changes: ({ relationName: string } & ({ insert: XgressRecord } | { delete: XgressRecord }))[]
  } = $props()

  // $effect(() => {
  //   // Initialize row array when pipelineName changes
  //   rows[pipelineName] ??= []
  // })

  // $effect(() => {
  //   const handle = relationEggressStream(pipelineName, relationName).then((stream) => {
  //     if (!stream) {
  //       return undefined
  //     }
  //     const reader = stream.getReader()
  //     accumulateChanges(reader, pushChanges)
  //     return () => reader.cancel('not_needed')
  //   })
  //   return () => {
  //     handle.then((cancel) => cancel?.())
  //   }
  // })

  // const bufferSize = 100
  // const pushChanges = (changes: Record<'insert' | 'delete', XgressRecord>[]) => {
  //   rows[pipelineName].splice(Math.max(bufferSize - changes.length, 0))
  //   rows[pipelineName].unshift(...changes.slice(-bufferSize).reverse())
  // }
  let ref: VList<any>
  let len = $derived(changes.length)
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
      class={`whitespace-nowrap pl-2 before:inline-block before:w-2 even:!bg-opacity-30 even:bg-surface-100-900 ` +
        ('insert' in item
          ? "shadow-[inset_26px_0px_0px_0px_rgba(0,255,0,0.3)] before:content-['+']"
          : 'delete' in item
            ? "shadow-[inset_26px_0px_0px_0px_rgba(255,0,0,0.3)] before:pl-[1px] before:content-['-']"
            : '')}
    >
      <span class="inline-block w-64 overflow-clip overflow-ellipsis pl-4">{item.relationName}</span
      >
      <span class="">{JSONbig.stringify((item as any).insert ?? (item as any).delete)}</span>
    </div>
  </VList>

  <!-- <div class="h-full overflow-auto">
    {#each changes as item}
      <div
        class={'even:bg-surface-100-900 border-l-4 pl-2 even:!bg-opacity-30 ' +
          (item.type === 'insert' ? '  border-green-500 ' : 'border-red-500')}>
        <span class="w-64 overflow-hidden overflow-ellipsis">{item.pipelineName}</span>
        {JSONbig.stringify(item.record)}
      </div>
    {/each}
  </div> -->
</div>
