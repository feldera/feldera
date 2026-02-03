<script lang="ts" module>
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import type { Field } from '$lib/services/manager'
  import type { SQLValueJS } from '$lib/types/sql'

  export type Row = { cells: SQLValueJS[] } | { error: string } | { warning: string }

  export type QueryResult = {
    rows: () => Row[]
    columns: Field[]
    totalSkippedBytes: number
    endResultStream: () => void
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
  import SQLValue from '$lib/components/relationData/SQLValue.svelte'
  import SqlColumnHeader from '$lib/components/relationData/SQLColumnHeader.svelte'
  import { usePopoverTooltip } from '$lib/compositions/common/usePopoverTooltip.svelte'
  import List from '$lib/components/common/virtualList/HeadlessVirtualList.svelte'
  import { useReverseScrollContainer } from '$lib/compositions/common/useReverseScrollContainer.svelte'
  import ScrollDownFab from '$lib/components/other/ScrollDownFab.svelte'
  import type { Snippet } from '$lib/types/svelte'
  import type { UIEventHandler } from 'svelte/elements'
  import { selectScope } from '$lib/compositions/common/userSelect'
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import ClipboardCopyButton from '../other/ClipboardCopyButton.svelte'
  import SQLValueTooltip from '../other/SQLValueTooltip.svelte'
  import { tableToCSJV } from '$lib/functions/sql'

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
    <div class="flex max-w-[1000px] flex-col rounded border border-surface-100-900 p-2">
      <div class="flex w-full flex-col gap-2">
        <textarea
          bind:value={query}
          style="font-family: {theme.config.monospaceFontFamily}; field-sizing: content"
          class="bg-white-dark scrollbar w-full overflow-auto rounded border-0 px-3 py-2 text-surface-950-50 outline-none"
          placeholder="SELECT * FROM ..."
          onkeydown={handleKeyDown(onSubmitQuery, disabled)}
        ></textarea>
        <div class="flex flex-nowrap items-center">
          <div class="flex h-10 flex-none">
            {#if progress}
              <button
                class="fd fd-square w-10 p-2 text-[20px]"
                onclick={onCancelQuery}
                aria-label="Stop query"
              ></button>
            {:else}
              <button
                {disabled}
                class="fd fd-play w-10 p-2 text-[20px]"
                onclick={() => onSubmitQuery(query)}
                aria-label="Run query"
              ></button>
            {/if}
            {#if !isLastQuery}
              <button
                class="fd fd-trash-2 w-10 p-2 text-[20px]"
                onclick={onDeleteQuery}
                aria-label="Delete query"
              ></button>
            {:else}
              <div class="w-10"></div>
            {/if}
          </div>
          <div class="flex h-6 w-full flex-nowrap items-center gap-4 whitespace-nowrap">
            {#if result}
              {@const len = rows.length}
              {len > 1 ? `${len} rows` : len === 0 ? 'No rows returned' : ''}
            {/if}
            {#if progress}
              <Progress class="h-1 max-w-[1000px]" value={null}>
                <Progress.Track>
                  <Progress.Range class="bg-primary-500" />
                </Progress.Track>
              </Progress>
            {/if}
          </div>
        </div>
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
                class="relative scrollbar h-full max-h-64 w-fit max-w-full overflow-auto rounded"
                use:reverseScroll.action
                {onscroll}
                bind:clientHeight={_.clientHeight}
              >
                <table style:height class="w-fit" use:keepMaxWidth>
                  {#if result.columns.length}
                    <thead>
                      <tr>
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
              {#if !row}{:else if 'cells' in row}
                <tr {style} class="{itemHeight} whitespace-nowrap odd:bg-white odd:dark:bg-black">
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
              {:else if 'error' in row}
                <tr {style} class={itemHeight} use:selectScope tabindex={-1}>
                  <td colspan="99999999" class="preset-tonal-error px-2">{row.error}</td>
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
            <ClipboardCopyButton class="-mr-4 mb-4" value={() => tableToCSJV(result)}
            ></ClipboardCopyButton>
          </div>
        </div>
      {/key}
    {/if}
  </div>
</div>
