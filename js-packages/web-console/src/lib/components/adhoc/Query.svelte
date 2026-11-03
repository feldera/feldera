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
  import SQLValue from '#lib/components/relationData/SQLValue.svelte'
  import SqlColumnHeader from '#lib/components/relationData/SQLColumnHeader.svelte'
  import { usePopoverTooltip } from '#lib/compositions/common/usePopoverTooltip.svelte.ts'
  import List from '#lib/components/common/virtualList/HeadlessVirtualList.svelte'
  import {
    ClipboardCopyButton,
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
    isLastQuery
  }: {
    onSubmitQuery: (query: string) => void
    onDeleteQuery: () => void
    onCancelQuery?: () => void
    disabled: boolean
    isLastQuery: boolean
  } & QueryData = $props()

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
  <div class="w-full">
    <!-- The query box: SQL on top, a toolbar with the actions and the result status below. -->
    <div
      class="flex max-w-[1000px] flex-col overflow-hidden rounded-lg border border-surface-200-800 transition-colors focus-within:border-surface-400-600"
    >
      <textarea
        bind:value={query}
        style="font-family: {theme.config.monospaceFontFamily}; field-sizing: content"
        class="scrollbar min-h-16 w-full resize-y overflow-auto border-0 bg-transparent px-3 py-2.5 text-surface-950-50 outline-none placeholder:text-surface-500 focus:ring-0"
        placeholder="SELECT * FROM ..."
        onkeydown={handleKeyDown(onSubmitQuery, disabled)}
      ></textarea>
      <div class="flex flex-nowrap items-center gap-3 border-t border-surface-200-800 px-2 py-2">
        {#if progress}
          <button
            class="btn h-8 preset-outlined-surface-200-800 px-3"
            onclick={onCancelQuery}
            aria-label="Stop query"
          >
            <span class="fd fd-square text-[14px]"></span>
            Stop
          </button>
        {:else}
          <button
            {disabled}
            class="btn h-8 preset-filled-primary-500 px-3"
            onclick={() => onSubmitQuery(query)}
            aria-label="Run query"
          >
            <span class="fd fd-play text-[14px]"></span>
            Run
          </button>
        {/if}
        <div class="flex h-6 min-w-0 flex-1 flex-nowrap items-center gap-4 whitespace-nowrap">
          {#if result}
            <span class="text-surface-600-400">{rowCount}</span>
          {/if}
          {#if progress}
            <Progress class="h-1 max-w-[1000px]" value={null}>
              <Progress.Track>
                <Progress.Range class="bg-secondary-500" />
              </Progress.Track>
            </Progress>
          {/if}
        </div>
        {#if !isLastQuery}
          <button
            class="fd fd-trash-2 btn-icon h-8 w-8 text-[16px] text-surface-600-400 hover:bg-row-hover hover:text-surface-950-50"
            onclick={onDeleteQuery}
            aria-label="Delete query"
          ></button>
        {/if}
      </div>
    </div>

    {#if result}
      {@const itemHeight = 'h-7'}
      {#key result.columns}
        <div class="pt-2 pr-4">
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
                class="relative scrollbar h-full max-h-64 w-fit max-w-full overflow-auto rounded pb-3"
                use:reverseScroll.action
                {onscroll}
                bind:clientHeight={_.clientHeight}
              >
                <table style:height class="w-fit" use:keepMaxWidth>
                  {#if result.columns.length}
                    <thead>
                      <tr>
                        <th class="bg-white-dark sticky top-0 z-10 pl-2 font-light {itemHeight}"
                          >#</th
                        >
                        {#each result.columns as column}
                          <SqlColumnHeader
                            {column}
                            class="bg-white-dark sticky top-0 z-10 {itemHeight}"
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
                <tr {style} class="{itemHeight} whitespace-nowrap odd:bg-white odd:dark:bg-black">
                  <td class="text-right font-mono select-none">{index}</td>
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
                    <div
                      class="flex min-h-7 items-start gap-1.5 rounded bg-status-error-subtle px-2 py-1 text-status-error-dot"
                    >
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
            <ClipboardCopyButton class="-mr-4 mb-2.5" value={() => tableToCSJV(result)}
            ></ClipboardCopyButton>
          </div>
        </div>
      {/key}
    {/if}
  </div>
</div>
