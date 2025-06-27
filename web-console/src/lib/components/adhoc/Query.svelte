<script lang="ts" module>
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import type { SQLValueJS } from '$lib/types/sql.ts'
  import type { Field } from '$lib/services/manager'
  import { Progress } from '@skeletonlabs/skeleton-svelte'

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
  import type { Snippet } from 'svelte'
  import type { UIEventHandler } from 'svelte/elements'
  import { selectScope } from '$lib/compositions/common/userSelect'

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
  let tooltip = usePopoverTooltip(() => popupRef)
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

<div
  class="bg-white-dark absolute m-0 w-max max-w-lg -translate-x-[4.5px] -translate-y-[2.5px] whitespace-break-spaces break-words border border-surface-500 px-2 py-1 text-surface-950-50"
  popover="manual"
  bind:this={popupRef}
  style={tooltip.data
    ? `left: ${tooltip.data.x}px; top: ${tooltip.data.y}px; min-width: ${tooltip.data.targetWidth + 8}px`
    : ''}
>
  {tooltip.data?.text}
</div>

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
    <div class="flex max-w-[1000px] flex-col rounded border p-2 border-surface-100-900">
      <div class="flex w-full flex-col gap-2">
        <textarea
          bind:value={query}
          style="font-family: {theme.config.monospaceFontFamily}; field-sizing: content"
          class="bg-white-dark w-full overflow-auto rounded border-0 !ring-0 !ring-primary-500 text-surface-950-50 scrollbar"
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
              <Progress value={null} meterBg="bg-primary-500" base="h-1 max-w-[1000px]"></Progress>
            {/if}
          </div>
        </div>
      </div>
    </div>

    {#if result}
      {@const itemHeight = 'h-7'}
      {#key result.columns}
        <div class="pr-4 pt-2">
          <div class="relative h-full w-fit max-w-full">
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
                class="relative h-full max-h-64 w-fit max-w-full overflow-auto rounded scrollbar"
                use:reverseScroll.action
                {onscroll}
                bind:clientHeight={_.clientHeight}
              >
                <table style:height class="w-fit" use:keepMaxWidth>
                  {#if result.columns.length}
                    <thead>
                      <tr>
                        <th class="bg-white-dark sticky top-0 z-10 {itemHeight}">#</th>
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
                  <td class="select-none text-right font-mono">{index}</td>
                  {#each row.cells as value}
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
              {:else if 'error' in row}
                <tr {style} class={itemHeight} use:selectScope tabindex={-1}>
                  <td colspan="99999999" class="px-2 preset-tonal-error">{row.error}</td>
                </tr>
              {:else}
                <tr {style} class={itemHeight} use:selectScope tabindex={-1}>
                  <td colspan="99999999" class="px-2 preset-tonal-warning">{row.warning}</td>
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
                  <tr class="h-0"></tr>
                {/snippet}
                {#snippet footer()}
                  <tr style="height: auto; ">
                    <td></td>
                  </tr>
                {/snippet}
              </List>
            {/if}
            <ScrollDownFab {reverseScroll}></ScrollDownFab>
          </div>
        </div>
      {/key}
    {/if}
  </div>
</div>
