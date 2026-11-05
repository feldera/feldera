<script lang="ts" module>
  import type { Field } from '#lib/services/manager/index.ts'
  import { isDataRow, isErrorRow, isWarningRow, type Row } from '#lib/types/adhocQuery.ts'
  import type { SQLValueJS } from '#lib/types/sql.ts'

  export type QueryResult = {
    rows: () => Row[]
    columns: Field[]
    totalSkippedBytes: number
    endResultStream: () => void
    /** The user stopped the query before its stream ended, so these rows are a prefix. */
    stopped?: boolean
  }

  export type QueryData = {
    query: string
    progress?: boolean
    result?: QueryResult
  }

  const handleKeyDown =
    (onSubmitQuery: (query: string) => void, disabled: boolean) => (e: KeyboardEvent) => {
      if (e.key === 'Enter') {
        if (e.shiftKey || e.altKey || e.ctrlKey) {
          return
        }
        if (!disabled) {
          onSubmitQuery((e as any).currentTarget.value)
        }
        e.preventDefault()
        return
      }
    }
</script>

<script lang="ts">
  import { errorRowClass } from 'common-ui/statusTone'
  import SQLValue from '#lib/components/relationData/SQLValue.svelte'
  import SqlColumnHeader from '#lib/components/relationData/SQLColumnHeader.svelte'
  import { usePopoverTooltip } from '#lib/compositions/common/usePopoverTooltip.svelte.ts'
  import List from '#lib/components/common/virtualList/HeadlessVirtualList.svelte'
  import {
    ClipboardCopyButton,
    Tooltip,
    ScrollDownFab,
    selectScope,
    useReverseScrollContainer
  } from 'common-ui'
  import type { Snippet } from '#lib/types/svelte.ts'
  import type { UIEventHandler } from 'svelte/elements'
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import SQLValueTooltip from '../other/SQLValueTooltip.svelte'
  import { tableToCSJV } from '#lib/functions/sql.ts'

  let {
    query = $bindable(),
    result,
    progress,
    onSubmitQuery,
    onDeleteQuery,
    onCancelQuery,
    disabled,
    isLastQuery,
    number = 1
  }: {
    onSubmitQuery: (query: string) => void
    onDeleteQuery: () => void
    onCancelQuery?: () => void
    disabled: boolean
    isLastQuery: boolean
    /** The query's position in the list, shown as "Ad-hoc query #n" once it has run. */
    number?: number
  } & QueryData = $props()

  // Folds the whole query, SQL and results, down to its header.
  let collapsed = $state(false)
  let showThreeSqlLines = $state(false)
  let wrapSql = $state(true)
  // Shared by the 24px icon buttons. Each button adds its own colour.
  const iconButtonClass =
    'btn-icon btn-icon-sm h-6 w-6 shrink-0 hover:bg-row-hover hover:text-surface-950-50'

  // Every result row, and the header row, is this tall. The virtual list depends on it.
  const ROW_HEIGHT_PX = 28

  // A click on a cell shows its full value in a popup.
  let popupRef: HTMLElement | undefined = $state()
  let tooltip = usePopoverTooltip<SQLValueJS>(() => popupRef)
  // The table only grows, so its columns do not jump as the virtual list scrolls other rows in.
  const keepMaxWidth = (element: HTMLElement) => {
    let maxWidth = 0
    const observer = new ResizeObserver(([entry]) => {
      maxWidth = Math.max(maxWidth, entry.borderBoxSize[0].inlineSize)
      element.style.minWidth = `${maxWidth}px`
    })
    observer.observe(element)
    return {
      destroy() {
        observer.disconnect()
      }
    }
  }
  let rows = $state<Row[]>([])
  $effect(() => {
    rows = result?.rows() ?? []
  })
  const reverseScroll = useReverseScrollContainer({ observeContentSize: () => rows.length })

  const returned = $derived(rows.filter(isDataRow).length)
  const failed = $derived(rows.some(isErrorRow))
  const truncated = $derived(rows.some(isWarningRow))
  const counted = $derived(returned === 1 ? '1 row' : `${returned} rows`)

  // A query streams its rows, so until it ends the count is a running total. If a failure, the
  // row cap or the user stops the result early, the count says so.
  const rowCount = $derived.by(() => {
    if (progress) {
      return returned === 0 ? 'No rows returned yet' : `${counted} so far`
    }
    if (failed) {
      // With no rows, the error row in the table is enough.
      return returned === 0 ? '' : `${counted}, then the query failed`
    }
    if (truncated) {
      return `First ${counted}`
    }
    if (result?.stopped) {
      return returned === 0 ? 'Stopped before any row arrived' : `${counted}, then stopped`
    }
    return returned === 0 ? 'No rows returned' : counted
  })
</script>

<SQLValueTooltip bind:popupRef tooltipData={tooltip.data}></SQLValueTooltip>

<div
  class="flex flex-nowrap items-start"
  role="presentation"
  onkeydown={(e) => {
    if (e.code === 'KeyC' && (e.ctrlKey || e.metaKey)) {
      onCancelQuery?.()
    }
  }}
>
  <!-- The block fits its content, which is the 1000px SQL box or wider results. -->
  <div
    class="flex w-fit max-w-full min-w-0 flex-col gap-2 rounded-lg border border-panel-line bg-surface-950-50/[0.03] p-2 dark:bg-black/20"
  >
    <!-- As tall as the 24px buttons, so Run is 8px from the top and from the right edge. -->
    <div class="flex h-6 w-full flex-nowrap items-center gap-2">
      {#if result}
        <button
          class="fd fd-chevron-down {iconButtonClass} text-[16px] text-surface-600-400 transition-transform {collapsed
            ? '-rotate-90'
            : ''}"
          onclick={() => (collapsed = !collapsed)}
          aria-label={collapsed ? 'Expand query' : 'Collapse query'}
          aria-expanded={!collapsed}
        ></button>
      {/if}
      <span class="shrink-0 font-medium text-surface-950-50">
        {result ? `Ad-hoc query #${number}` : 'Add an ad-hoc query'}
      </span>
      {#if result && rowCount}
        <span class="text-surface-500" aria-hidden="true">·</span>
        <span class="shrink-0 whitespace-nowrap text-surface-600-400">{rowCount}</span>
      {/if}
      {#if collapsed}
        <span class="min-w-0 truncate font-mono text-sm text-surface-500"
          >{query.split('\n')[0]}</span
        >
      {/if}
      <div class="flex min-w-0 flex-1 items-center">
        {#if progress}
          <Progress class="h-1 max-w-[240px]" value={null}>
            <Progress.Track>
              <Progress.Range class="bg-secondary-500" />
            </Progress.Track>
          </Progress>
        {/if}
      </div>
      {#if result && result.columns.length && !collapsed}
        <!-- Copies the results, as comma-separated JSON values. -->
        <ClipboardCopyButton
          class="{iconButtonClass} text-[14px] text-surface-600-400"
          value={() => tableToCSJV(result)}
        ></ClipboardCopyButton>
        <Tooltip placement="top">Copy results</Tooltip>
      {/if}
      {#if !isLastQuery}
        <button
          class="fd fd-trash-2 {iconButtonClass} text-[14px] text-surface-600-400"
          onclick={onDeleteQuery}
          aria-label="Delete query"
        ></button>
      {/if}
      {#if progress}
        <button
          class="btn h-6 preset-outlined-surface-200-800 btn-sm px-2.5"
          onclick={onCancelQuery}
          aria-label="Stop query"
        >
          <span class="fd fd-square text-[12px]"></span>
          Stop
        </button>
      {:else}
        <button
          {disabled}
          class="btn h-6 preset-filled-primary-500 btn-sm px-2.5"
          onclick={() => onSubmitQuery(query)}
          aria-label="Run query"
        >
          <span class="fd fd-play text-[12px]"></span>
          Run
        </button>
      {/if}
    </div>
    {#if !collapsed}
      <div
        class="relative flex w-[1000px] max-w-full flex-col overflow-hidden rounded border border-surface-200-800 bg-panel transition-colors focus-within:border-surface-400-600"
      >
        <!-- The right padding keeps the SQL clear of the two 24px buttons in the corner. -->
        <textarea
          bind:value={query}
          style="field-sizing: content"
          wrap={wrapSql ? 'soft' : 'off'}
          class="scrollbar min-h-16 w-full overflow-auto border-0 bg-transparent py-2.5 pr-16 pl-3 font-mono text-md text-surface-950-50 outline-none placeholder:text-surface-500 focus:ring-0 {showThreeSqlLines
            ? 'max-h-[80px] resize-none'
            : 'resize-y'} {wrapSql ? '' : 'whitespace-pre'}"
          placeholder="SELECT * FROM ..."
          onkeydown={handleKeyDown(onSubmitQuery, disabled)}
        ></textarea>
        <div class="absolute top-1.5 right-1.5 flex items-center gap-0.5">
          <button
            type="button"
            class="fd fd-wrap-text {iconButtonClass} text-[14px] {wrapSql
              ? 'text-surface-950-50'
              : 'text-surface-500'}"
            onclick={() => (wrapSql = !wrapSql)}
            aria-label={wrapSql ? "Don't wrap lines" : 'Wrap lines'}
            aria-pressed={wrapSql}
            title={wrapSql ? "Don't wrap lines" : 'Wrap lines'}
          ></button>
          <button
            type="button"
            class="fd {showThreeSqlLines
              ? 'fd-maximize-2'
              : 'fd-minimize-2'} {iconButtonClass} text-[14px] text-surface-500"
            onclick={() => (showThreeSqlLines = !showThreeSqlLines)}
            aria-label={showThreeSqlLines ? 'Show all lines' : 'Show three lines'}
            title={showThreeSqlLines ? 'Show all lines' : 'Show three lines'}
          ></button>
        </div>
      </div>
    {/if}

    {#if result && !collapsed}
      {@const rowHeight = `height: ${ROW_HEIGHT_PX}px`}
      {#key result.columns}
        <div class="relative w-fit max-w-full pr-4">
          {#snippet listContainer(
            items: Snippet,
            {
              height,
              onscroll,
              setClientHeight
            }: {
              height?: string
              onscroll?: UIEventHandler<HTMLDivElement>
              setClientHeight?: (value: number) => void
            }
          )}
            <!-- 80cqh is 80% of the ad-hoc tab's height, because TabAdHocQuery.svelte makes the
                 tab a size container. -->
            <div
              class="relative scrollbar max-h-[80cqh] w-fit max-w-full overflow-auto rounded border border-panel-line"
              use:reverseScroll.action
              {onscroll}
              bind:clientHeight={() => 0, (value) => setClientHeight?.(value)}
            >
              <!-- The lines between rows are inset shadows, because borders would make the rows
                   taller than ROW_HEIGHT_PX. -->
              <table
                style:height
                class="w-fit border-separate border-spacing-0 [&_tbody_tr+tr>td]:shadow-[inset_0_1px_0_var(--color-panel-line)] [&_th]:shadow-[inset_0_-1px_0_var(--color-panel-line)]"
                use:keepMaxWidth
              >
                {#if result.columns.length}
                  <thead>
                    <tr>
                      <th
                        class="sticky top-0 z-10 bg-panel px-3 text-right font-normal text-surface-500"
                        style={rowHeight}>#</th
                      >
                      {#each result.columns as column}
                        <SqlColumnHeader
                          {column}
                          class="sticky top-0 z-10 bg-panel"
                          style={rowHeight}
                        ></SqlColumnHeader>
                      {/each}
                    </tr>
                  </thead>
                {/if}
                <tbody>
                  {@render items()}
                </tbody>
              </table>
            </div>
          {/snippet}
          {#snippet item({ index, style }: { index: number; style?: string })}
            {@const row = rows[index]}
            {#if !row}{:else if isDataRow(row)}
              <tr style="{rowHeight}; {style}" class="whitespace-nowrap hover:bg-row-hover">
                <td class="px-3 text-right text-surface-500 tabular-nums select-none">{index}</td>
                {#each row.cells as value}
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
            {:else if isErrorRow(row)}
              <tr style="{rowHeight}; {style}" use:selectScope tabindex={-1}>
                <td colspan={result.columns.length + 1}>
                  <div
                    class="flex items-start gap-1.5 rounded px-2 py-1 {errorRowClass}"
                    style="min-height: {ROW_HEIGHT_PX}px"
                  >
                    <span class="fd fd-circle-x mt-[3px] shrink-0 text-[14px]"></span>
                    <span class="inline-block whitespace-pre-wrap first-letter:uppercase"
                      >{row.error}</span
                    >
                  </div>
                </td>
              </tr>
            {:else}
              <tr style="{rowHeight}; {style}" use:selectScope tabindex={-1}>
                <td colspan={result.columns.length + 1} class="preset-tonal-warning px-2"
                  >{row.warning}</td
                >
              </tr>
            {/if}
          {/snippet}
          <List
            itemSize={ROW_HEIGHT_PX}
            itemCount={rows.length}
            marginTop={result.columns.length ? ROW_HEIGHT_PX : 0}
            {listContainer}
            {item}
          >
            <!-- The list requires this row for zebra stripes. The result rows have no stripes. -->
            {#snippet emptyItem()}
              <tr class="hidden"></tr>
            {/snippet}
            <!-- The table is as tall as all the rows, but the list renders only the visible ones.
                 This row takes the remaining height, so the rendered rows stay ROW_HEIGHT_PX
                 tall. -->
            {#snippet footer()}
              <tr style="height: auto">
                <td></td>
              </tr>
            {/snippet}
          </List>
          <!-- The button is positioned in the wrapper above, which includes the 16px right
               padding. -->
          <ScrollDownFab class="mr-11" {reverseScroll}></ScrollDownFab>
        </div>
      {/key}
    {/if}
  </div>
</div>
