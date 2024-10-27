<script lang="ts">
  import JSONbig from 'true-json-bigint'

  import { humanSize } from '$lib/functions/common/string'
  import type { XgressEntry } from '$lib/services/pipelineManager'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import ReverseScrollList from './ReverseScrollList.svelte'

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
</script>

<div class="relative flex w-full flex-1 flex-col">
  {#if changeStream.totalSkippedBytes}
    <WarningBanner>
      Receiving changes faster than can be displayed. Skipping some records to keep up, {humanSize(
        changeStream.totalSkippedBytes
      )} in total.
    </WarningBanner>
  {/if}
  <ReverseScrollList items={changeStream.rows}>
    {#snippet item(item)}
      <div
        oncopy={(e) => {
          e.clipboardData!.setData('text/plain', JSONbig.stringify(item))
          e.preventDefault()
        }}
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
  </ReverseScrollList>
</div>
