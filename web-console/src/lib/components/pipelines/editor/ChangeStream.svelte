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
  import ReverseScrollFixedList from '$lib/components/pipelines/editor/ReverseScrollFixedList.svelte'
  import SqlValue from '$lib/components/relationData/SQLValue.svelte'
  import type { Field } from '$lib/services/manager'
  import SqlColumnHeader from '$lib/components/relationData/SQLColumnHeader.svelte'
  import { usePopoverTooltip } from '$lib/compositions/common/usePopoverTooltip.svelte'

  let {
    changeStream
  }: {
    changeStream: ChangeStreamData
  } = $props()

  let popupRef: HTMLElement | undefined = $state()
  let tooltip = usePopoverTooltip(() => popupRef)
</script>

<div
  class="bg-white-black absolute m-0 w-max max-w-lg -translate-x-[4.5px] -translate-y-[2.5px] whitespace-break-spaces break-words border border-surface-500 px-2 py-1 text-right text-surface-950-50"
  popover="manual"
  bind:this={popupRef}
  style={tooltip.data
    ? `left: ${tooltip.data.x}px; top: ${tooltip.data.y}px; min-width: ${tooltip.data.targetWidth + 8}px`
    : ''}
>
  {tooltip.data?.text}
</div>

<div class="bg-white-black relative flex w-full flex-1 flex-col">
  {#if changeStream.totalSkippedBytes}
    <WarningBanner>
      Receiving changes faster than can be displayed. Skipping some records to keep up, {humanSize(
        changeStream.totalSkippedBytes
      )} in total.
    </WarningBanner>
  {/if}
  <ReverseScrollFixedList
    itemSize={28}
    items={changeStream.rows}
    class="overflow-scroll scrollbar"
    stickyIndices={changeStream.headers}
  >
    {#snippet listContainer(children, { height, onscroll, onresize, setref })}
      {@const _height = {
        set current(x: number) {
          onresize({ clientHeight: x })
        }
      }}
      {@const ref = {
        set current(el: HTMLElement) {
          setref(el)
        }
      }}
      <div
        class="h-full overflow-auto scrollbar"
        {onscroll}
        bind:clientHeight={_height.current}
        bind:this={ref.current}
      >
        <table style:height>
          <tbody>
            {@render children()}
          </tbody>
        </table>
      </div>
    {/snippet}
    {#snippet item(row, style, padding, isSticky)}
      {#if 'skippedBytes' in row}
        <tr class="h-7" style="{style} {padding}">
          <td colspan="99">
            <span>{`Skipped ${humanSize(row.skippedBytes)} of changes stream`}</span>
          </td>
        </tr>
      {:else if 'columns' in row}
        <tr class="h-7" style="{style} {padding}">
          <th class="pl-2 font-normal {isSticky ? 'bg-white-black sticky top-0 z-10' : ''}"
            >{row.relationName}</th
          >
          {#each row.columns as column}
            <SqlColumnHeader {column} {isSticky} class="bg-white-black px-2"></SqlColumnHeader>
          {/each}
        </tr>
      {:else}
        {@const data = 'insert' in row ? row.insert : row.delete}
        <tr
          style="{style} {padding}"
          class={`h-7 whitespace-nowrap even:bg-surface-50-950`}
          oncopy={(e) => {
            e.clipboardData!.setData('text/plain', JSONbig.stringify(item))
            e.preventDefault()
          }}
        >
          {#if 'insert' in row}
            <td class="block h-7 w-8 bg-opacity-30 pt-1 text-center font-mono bg-success-100-900"
              >+</td
            >
          {:else}
            <td class="block h-7 w-8 bg-opacity-30 text-center font-mono bg-error-100-900">-</td>
          {/if}
          {#each Object.values(data) as value}
            <SqlValue
              {value}
              props={(format) => ({
                onclick: tooltip.showTooltip(format(value)),
                onmouseleave: tooltip.onmouseleave
              })}
            ></SqlValue>
          {/each}
        </tr>
      {/if}
    {/snippet}
    {#snippet footer()}
      <tr style="height: auto; ">
        <td></td>
      </tr>
    {/snippet}
  </ReverseScrollFixedList>
</div>
