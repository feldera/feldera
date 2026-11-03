<script lang="ts" module>
  import type { XgressEntry } from '#lib/services/pipelineManager.ts'
  export type Row =
    | { relationName: string; columns: Field[] }
    | XgressEntry
    | { relationName: string; skippedBytes: number }
  export type ChangeStreamData = {
    rows: Row[]
    headers: number[]
    totalSkippedBytes: number
  }
</script>

<script lang="ts">
  import { statusToneColors } from 'common-ui/statusTone'
  import { humanSize } from 'common-lib/string'
  import JSONbig from 'true-json-bigint'

  import WarningBanner from '#lib/components/pipelines/editor/WarningBanner.svelte'
  import List from '#lib/components/common/virtualList/HeadlessVirtualList.svelte'
  import SQLValue from '#lib/components/relationData/SQLValue.svelte'
  import type { Field } from '#lib/services/manager/index.ts'
  import SqlColumnHeader from '#lib/components/relationData/SQLColumnHeader.svelte'
  import { usePopoverTooltip } from '#lib/compositions/common/usePopoverTooltip.svelte.ts'
  import { ScrollDownFab, useReverseScrollContainer } from 'common-ui'
  import SQLValueTooltip from '#lib/components/other/SQLValueTooltip.svelte'
  import type { SQLValueJS } from '#lib/types/sql.ts'

  let {
    changeStream,
    onScrollPausedChange
  }: {
    changeStream: ChangeStreamData
    /** Fired whenever scroll-pause toggles: `true` when the user scrolls up off the
     *  bottom, `false` when the view sticks to the bottom again. */
    onScrollPausedChange?: (paused: boolean) => void
  } = $props()

  let popupRef: HTMLElement | undefined = $state()
  let tooltip = usePopoverTooltip<SQLValueJS>(() => popupRef)

  // Pause the stream when the view stops sticking to the bottom (scrolled up off it) and
  // resume when it sticks again — the same threshold that toggles `ScrollDownFab`, so
  // pause/resume and FAB visibility stay in lockstep. Driven by the scroll container's own
  // notification rather than a reactive read, so the host callback always fires.
  // The table sizes its columns to the rows on screen, which change with every batch, so
  // columns would jump wider and narrower as the stream flows. Each column instead keeps the
  // widest width it has had, per relation, so new rows only ever widen it.
  const columnWidths = new Map<string, number[]>()
  const growOnlyColumns = (row: HTMLElement, relationName: string) => {
    const observer = new ResizeObserver(() => {
      const widths = columnWidths.get(relationName) ?? []
      ;[...row.children].forEach((cell, i) => {
        const width = (cell as HTMLElement).getBoundingClientRect().width
        if (width > (widths[i] ?? 0)) {
          widths[i] = width
        }
        ;(cell as HTMLElement).style.minWidth = `${widths[i]}px`
      })
      columnWidths.set(relationName, widths)
    })
    ;[...row.children].forEach((cell) => observer.observe(cell))
    return {
      destroy: () => observer.disconnect()
    }
  }

  const reverseScroll = useReverseScrollContainer({
    observeContentSize: () => changeStream.rows.length,
    onStickToBottomChange: (stickToBottom) => onScrollPausedChange?.(!stickToBottom)
  })
</script>

<SQLValueTooltip bind:popupRef tooltipData={tooltip.data}></SQLValueTooltip>

<div class="relative flex w-full flex-1 flex-col">
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
        class="scrollbar h-full overflow-auto"
        use:reverseScroll.action
        {onscroll}
        bind:clientHeight={_.clientHeight}
      >
        <!-- Lines between rows instead of stripes: the list renders only the rows on screen,
             so stripes would swap with every new row and make the table flicker. The lines are
             inset shadows, so every row stays exactly 28px. -->
        <table
          style:height
          class="border-separate border-spacing-0 text-[13px] [&_td]:shadow-[inset_0_-1px_0_var(--color-line)] dark:[&_td]:shadow-[inset_0_-1px_0_#3f3f3f] [&_th]:shadow-[inset_0_-1px_0_var(--color-line)] dark:[&_th]:shadow-[inset_0_-1px_0_#3f3f3f]"
        >
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
        <tr class="h-7" style="{style} {padding}" use:growOnlyColumns={row.relationName}>
          <!-- The header sits on the panel colour and keeps it while pinned to the top. -->
          <th
            class="bg-white px-3 text-left text-[12px] font-semibold dark:bg-[#2a2a2a] {isSticky
              ? 'sticky top-0 z-10'
              : ''}">{row.relationName}</th
          >
          {#each row.columns as column}
            <SqlColumnHeader {column} {isSticky} class="bg-white dark:bg-[#2a2a2a]"
            ></SqlColumnHeader>
          {/each}
        </tr>
      {:else}
        {@const data = 'insert' in row ? row.insert : row.delete}
        <tr
          data-testid="box-changestream-row"
          style="{style} {padding}"
          class="h-7 whitespace-nowrap select-none hover:bg-row-hover"
          oncopy={(e) => {
            e.clipboardData!.setData('text/plain', JSONbig.stringify(row))
            e.preventDefault()
          }}
        >
          <!-- A small chip in the status colours marks whether the change adds or removes a row. -->
          <td class="w-20 px-3">
            {#if 'insert' in row}
              <span
                class="inline-flex h-5 items-center rounded px-1.5 text-xs font-medium {statusToneColors
                  .jade.chip}">Insert</span
              >
            {:else}
              <span
                class="inline-flex h-5 items-center rounded px-1.5 text-xs font-medium {statusToneColors
                  .error.chip}">Delete</span
              >
            {/if}
          </td>

          {#each Object.values(data) as value}
            <SQLValue
              {value}
              class="cursor-pointer"
              props={{
                onclick: tooltip.showTooltip(value),
                onmouseleave: tooltip.onmouseleave
              }}
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
