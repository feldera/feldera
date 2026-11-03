<script lang="ts" module>
  import { useSkeletonTheme } from '#lib/compositions/useSkeletonTheme.svelte.ts'
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
      // Enter to submit, Shift + Enter to enter newline
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

  // The chevron folds the whole query (SQL and results) down to its header.
  let collapsed = $state(false)
  // The SQL can be limited to three lines, and its long lines wrapped or scrolled sideways.
  let sqlMinimized = $state(false)
  let wrapSql = $state(true)

  const theme = useSkeletonTheme()

  // Handle hover popup over table cells to display full SQL value
  let popupRef: HTMLElement | undefined = $state()
  let tooltip = usePopoverTooltip<SQLValueJS>(() => popupRef)
  const keepMaxWidth = (element: HTMLElement) => {
    const observer = new ResizeObserver(([entry]) => {
      maxW = Math.max(maxW, entry.borderBoxSize[0].inlineSize)
      element.style.minWidth = `${maxW}px`
    })
    observer.observe(element)
    let maxW = $state(0)
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

  // A query streams its rows, so until it ends the count is a running total. A result cut short,
  // by a failure, by the row cap or by the user, is explicit.
  const rowCount = $derived.by(() => {
    if (progress) {
      return returned === 0 ? 'No rows returned yet' : `${counted} so far`
    }
    if (failed) {
      // With no rows, the failure message the table prints says it all.
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
  <!-- Each query and its results form one block on a darker background that hugs its
       content: the 1000px SQL box, or wider results. -->
  <div
    class="flex w-fit max-w-full min-w-0 flex-col gap-2 rounded-xl border border-line bg-surface-950-50/[0.03] p-2 dark:border-[#3f3f3f] dark:bg-black/20"
  >
    <!-- A header on the block's darker background with the title, the result status and the
         actions; then the framed SQL, which can collapse once the query has run; then results. -->
    <div class="flex h-8 w-full flex-nowrap items-center gap-2">
      {#if result}
        <button
          class="fd fd-chevron-down btn-icon btn-icon-sm h-6 w-6 shrink-0 text-[16px] text-surface-600-400 transition-transform hover:bg-row-hover hover:text-surface-950-50 {collapsed
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
        <span
          class="min-w-0 truncate text-sm text-surface-500"
          style="font-family: {theme.config.monospaceFontFamily}">{query.split('\n')[0]}</span
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
          class="btn-icon-sm h-6 w-6 shrink-0 text-[14px] text-surface-600-400 hover:bg-row-hover hover:text-surface-950-50"
          value={() => tableToCSJV(result)}
        ></ClipboardCopyButton>
        <Tooltip placement="top">Copy results</Tooltip>
      {/if}
      {#if !isLastQuery}
        <button
          class="fd fd-trash-2 btn-icon btn-icon-sm h-6 w-6 shrink-0 text-[14px] text-surface-600-400 hover:bg-row-hover hover:text-surface-950-50"
          onclick={onDeleteQuery}
          aria-label="Delete query"
        ></button>
      {/if}
      <!-- Run (or Stop) always sits at the right edge; the other actions sit to its left. -->
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
        class="relative flex w-[1000px] max-w-full flex-col overflow-hidden rounded-lg border border-surface-200-800 bg-white transition-colors focus-within:border-surface-400-600 dark:bg-[#2a2a2a]"
      >
        <textarea
          bind:value={query}
          style="font-family: {theme.config.monospaceFontFamily}; field-sizing: content"
          wrap={wrapSql ? 'soft' : 'off'}
          class="scrollbar min-h-16 w-full overflow-auto border-0 bg-transparent py-2.5 pr-16 pl-3 text-[13px] leading-5 text-surface-950-50 outline-none placeholder:text-surface-500 focus:ring-0 {sqlMinimized
            ? 'max-h-[80px] resize-none'
            : 'resize-y'} {wrapSql ? '' : 'whitespace-pre'}"
          placeholder="SELECT * FROM ..."
          onkeydown={handleKeyDown(onSubmitQuery, disabled)}
        ></textarea>
        <!-- View controls in the box's top-right corner: wrap long lines, and show at most three
             lines of SQL. -->
        <div class="absolute top-1.5 right-1.5 flex items-center gap-0.5">
          <button
            type="button"
            class="btn-icon btn-icon-sm h-6 w-6 text-surface-500 hover:bg-row-hover hover:text-surface-950-50 {wrapSql
              ? 'text-surface-950-50'
              : ''}"
            onclick={() => (wrapSql = !wrapSql)}
            aria-label={wrapSql ? "Don't wrap lines" : 'Wrap lines'}
            aria-pressed={wrapSql}
            title={wrapSql ? "Don't wrap lines" : 'Wrap lines'}
          >
            <svg
              viewBox="0 0 24 24"
              class="h-3.5 w-3.5"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
              aria-hidden="true"
              ><path d="M3 6h18M3 12h15a3 3 0 1 1 0 6h-4M3 18h7" /><path d="m16 16-2 2 2 2" /></svg
            >
          </button>
          <button
            type="button"
            class="btn-icon btn-icon-sm h-6 w-6 text-surface-500 hover:bg-row-hover hover:text-surface-950-50"
            onclick={() => (sqlMinimized = !sqlMinimized)}
            aria-label={sqlMinimized ? 'Show all lines' : 'Show three lines'}
            title={sqlMinimized ? 'Show all lines' : 'Show three lines'}
          >
            <svg
              viewBox="0 0 24 24"
              class="h-3.5 w-3.5"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
              aria-hidden="true"
            >
              {#if sqlMinimized}
                <path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7" />
              {:else}
                <path d="M4 14h6v6M20 10h-6V4M14 10l7-7M3 21l7-7" />
              {/if}
            </svg>
          </button>
        </div>
      </div>
    {/if}

    {#if result && !collapsed}
      {@const itemHeight = 'h-7'}
      {#key result.columns}
        <div class="pr-4">
          <div class="relative flex h-full w-fit max-w-full flex-nowrap items-end">
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
              {@const _ = {
                set clientHeight(value: number) {
                  setClientHeight?.(value)
                }
              }}
              <div
                class="relative scrollbar h-full max-h-[80cqh] w-fit max-w-full overflow-auto rounded-lg border border-line dark:border-[#3f3f3f]"
                use:reverseScroll.action
                {onscroll}
                bind:clientHeight={_.clientHeight}
              >
                <!-- A framed table with a line between rows. The lines are inset shadows, so
                     every row stays exactly 28px for the virtual list. -->
                <table
                  style:height
                  class="w-fit border-separate border-spacing-0 [&_tbody_tr+tr>td]:shadow-[inset_0_1px_0_var(--color-line)] dark:[&_tbody_tr+tr>td]:shadow-[inset_0_1px_0_#3f3f3f] [&_th]:shadow-[inset_0_-1px_0_var(--color-line)] dark:[&_th]:shadow-[inset_0_-1px_0_#3f3f3f]"
                  use:keepMaxWidth
                >
                  {#if result.columns.length}
                    <thead>
                      <tr>
                        <th
                          class="sticky top-0 z-10 bg-white px-3 text-right font-normal text-surface-500 dark:bg-[#2a2a2a] {itemHeight}"
                          >#</th
                        >
                        {#each result.columns as column}
                          <SqlColumnHeader
                            {column}
                            class="sticky top-0 z-10 bg-white dark:bg-[#2a2a2a] {itemHeight}"
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
                <tr {style} class="{itemHeight} whitespace-nowrap hover:bg-row-hover">
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
                <tr {style} class={itemHeight} use:selectScope tabindex={-1}>
                  <td colspan="99999999">
                    <!-- At least one 28px row like the data rows; a long error wraps below. -->
                    <div class="flex min-h-7 items-start gap-1.5 rounded px-2 py-1 {errorRowClass}">
                      <span class="fd fd-circle-x mt-[3px] shrink-0 text-[14px]"></span>
                      <span class="inline-block whitespace-pre-wrap first-letter:uppercase"
                        >{row.error}</span
                      >
                    </div>
                  </td>
                </tr>
              {:else}
                <tr {style} class={itemHeight} use:selectScope tabindex={-1}>
                  <td colspan="99999999" class="preset-tonal-warning px-2">{row.warning}</td>
                </tr>
              {/if}
            {/snippet}
            {#if rows.length < 2}
              {#snippet items()}
                {#each rows as _, i}
                  {@render item({ index: i })}
                {/each}
              {/snippet}
              {@render listContainer(items, {})}
            {:else}
              <List
                itemSize={28}
                itemCount={rows.length}
                stickyIndices={[]}
                marginTop={28}
                {listContainer}
                {item}
              >
                {#snippet emptyItem()}
                  <tr class="hidden"></tr>
                {/snippet}
                {#snippet footer()}
                  <tr style="height: auto; ">
                    <td></td>
                  </tr>
                {/snippet}
              </List>
            {/if}
            <ScrollDownFab class="mr-7" {reverseScroll}></ScrollDownFab>
          </div>
        </div>
      {/key}
    {/if}
  </div>
</div>
