<script lang="ts" module>
</script>

<script lang="ts">
  import JSONbig from 'true-json-bigint'

  import { VirtualList } from 'svelte-virtuallists'
  import { scale } from 'svelte/transition'
  import { humanSize } from '$lib/functions/common/string'
  import type { XgressEntry } from '$lib/services/pipelineManager'
  import {
    useElementSize,
    useStickScrollToBottom
  } from '$lib/compositions/components/virtualList.svelte'

  type Payload = XgressEntry | { skippedBytes: number }
  type Row = { relationName: string } & Payload
  let {
    changeStream
  }: {
    changeStream: {
      rows: Row[]
      totalSkippedBytes: number
    }
  } = $props()

  const itemSize = 24
  let ref = $state<HTMLElement>()
  let size = useElementSize(() => ref)

  const scrollProps = useStickScrollToBottom(
    () => size.height,
    () => changeStream.rows,
    itemSize
  )
</script>

<div class="relative flex flex-1 flex-col" bind:this={ref}>
  {#if changeStream.totalSkippedBytes}
    <div class="flex gap-1 p-1 preset-tonal-warning">
      <span class="fd fd-warning_amber text-[24px]"></span>
      <span>
        Receiving changes faster than can be displayed. Skipping some records to keep up, {humanSize(
          changeStream.totalSkippedBytes
        )} in total.
      </span>
    </div>
  {/if}

  <VirtualList
    width="100%"
    height={size.height}
    scrollOffset={scrollProps.scrollOffset}
    onAfterScroll={scrollProps.onAfterScroll}
    model={changeStream.rows}
    modelCount={changeStream.rows.length}
    {itemSize}
  >
    {#snippet slot({ item, style }: { item: Row; style: string })}
      <div
        oncopy={(e) => {
          e.clipboardData!.setData('text/plain', JSONbig.stringify(item))
          e.preventDefault()
        }}
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
        <span class=""
          >{'insert' in item
            ? JSONbig.stringify(item.insert)
            : 'delete' in item
              ? JSONbig.stringify(item.delete)
              : `Skipped ${humanSize(item.skippedBytes)} of changes stream`}</span
        >
      </div>
    {/snippet}
  </VirtualList>
  {#if !scrollProps.isAtBottom}
    <button
      transition:scale={{ duration: 200 }}
      class="fd fd-arrow_downward absolute bottom-4 right-4 rounded-full p-2 text-[24px] preset-filled-primary-500"
      onclick={scrollProps.scrolToBottom}
    ></button>
  {/if}
</div>
