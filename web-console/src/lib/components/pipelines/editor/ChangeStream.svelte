<script lang="ts" module>
  import type { XgressEntry } from '$lib/services/pipelineManager'
  export type Row =
    | { relationName: string; columns: Field[] }
    | XgressEntry
    | { skippedBytes: number }
  export type ChangeStreamData = {
    rows: Row[]
    headers: number[]
    totalSkippedBytes: number
  }
</script>

<script lang="ts">
  import JSONbig from 'true-json-bigint'

  import { humanSize } from '$lib/functions/common/string'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import List from '$lib/components/common/virtualList/HeadlessVirtualList.svelte'
  import SQLValue from '$lib/components/relationData/SQLValue.svelte'
  import type { Field } from '$lib/services/manager'
  import SqlColumnHeader from '$lib/components/relationData/SQLColumnHeader.svelte'
  import { usePopoverTooltip } from '$lib/compositions/common/usePopoverTooltip.svelte'
  import { useReverseScrollContainer } from '$lib/compositions/common/useReverseScrollContainer.svelte'
  import ScrollDownFab from '$lib/components/other/ScrollDownFab.svelte'

  let {
    changeStream
  }: {
    changeStream: ChangeStreamData
  } = $props()

  let popupRef: HTMLElement | undefined = $state()
  let tooltip = usePopoverTooltip(() => popupRef)

  const reverseScroll = useReverseScrollContainer()
</script>

<div
  class="bg-white-dark absolute m-0 max-h-[90%] w-max max-w-lg -translate-x-[4.5px] -translate-y-[2.5px] overflow-auto border border-surface-500 px-2 py-2 scrollbar"
  popover="manual"
  bind:this={popupRef}
  style={tooltip.data
    ? `left: ${tooltip.data.x}px; top: ${tooltip.data.y}px; min-width: ${tooltip.data.targetWidth + 8}px`
    : ''}
>
  <div class="whitespace-break-spaces break-words text-surface-950-50">
    {tooltip.data?.text}
  </div>
</div>

<div class="bg-white-dark relative flex w-full flex-1 flex-col rounded">
  {#if changeStream.totalSkippedBytes}
    <WarningBanner>
      Receiving changes faster than can be displayed. Skipping some records to keep up, {humanSize(
        changeStream.totalSkippedBytes
      )} in total.
    </WarningBanner>
  {/if}
  <List itemSize={28} itemCount={changeStream.rows.length} stickyIndices={changeStream.headers}>
    {#snippet listContainer(children, { height, onscroll, setClientHeight })}
      {@const _ = {
        set clientHeight(value: number) {
          setClientHeight(value)
        }
      }}
      <div
        class="h-full overflow-auto scrollbar"
        use:reverseScroll.action={{ observeContentSize: () => changeStream.rows.length }}
        {onscroll}
        bind:clientHeight={_.clientHeight}
      >
        <table style:height class="">
          <tbody>
            {@render children()}
          </tbody>
        </table>
      </div>
    {/snippet}
    {#snippet item({ index, style, padding, isSticky })}
      {@const row = changeStream.rows[index]}
      {#if !row}{:else if 'skippedBytes' in row}
        <tr class="h-7" style="{style} {padding}">
          <td colspan="99">
            <span>{`Skipped ${humanSize(row.skippedBytes)} of changes stream`}</span>
          </td>
        </tr>
      {:else if 'columns' in row}
        <tr class="h-7" style="{style} {padding}">
          <th class="pl-2 font-normal {isSticky ? 'bg-white-dark sticky top-0 z-10' : ''}"
            >{row.relationName}</th
          >
          {#each row.columns as column}
            <SqlColumnHeader {column} {isSticky} class="bg-white-dark px-2"></SqlColumnHeader>
          {/each}
        </tr>
      {:else}
        {@const data = 'insert' in row ? row.insert : row.delete}
        <tr
          style="{style} {padding}"
          class="h-7 select-none whitespace-nowrap even:bg-surface-50-950"
          oncopy={(e) => {
            e.clipboardData!.setData('text/plain', JSONbig.stringify(row))
            e.preventDefault()
          }}
        >
          {#if 'insert' in row}
            <td class="block h-7 w-20 bg-opacity-30 pt-1 text-center font-mono bg-success-100-900">
              Insert
            </td>
          {:else}
            <td class="block h-7 w-20 bg-opacity-30 pt-1 text-center font-mono bg-error-100-900">
              Delete
            </td>
          {/if}

          {#each Object.values(data) as value}
            <SQLValue
              {value}
              class="cursor-pointer"
              props={(format) => ({
                onclick: tooltip.showTooltip(format(value)),
                onmouseleave: tooltip.onmouseleave
              })}
            ></SQLValue>
          {/each}
        </tr>
      {/if}
    {/snippet}
    {#snippet emptyItem()}
      <tr class="hidden"></tr>
    {/snippet}
    {#snippet footer()}
      <tr style="height: auto; ">
        <td></td>
      </tr>
    {/snippet}
  </List>
  <ScrollDownFab {reverseScroll}></ScrollDownFab>
</div>
