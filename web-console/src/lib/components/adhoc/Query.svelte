<script lang="ts" module>
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import type { SQLValueJS } from '$lib/functions/sqlValue'
  import type { Field } from '$lib/services/manager'
  import { Progress } from '@skeletonlabs/skeleton-svelte'

  export type Row = { cells: SQLValueJS[] } | { error: string } | { warning: string }

  export type QueryResult = {
    rows: Row[]
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
        if (!e.shiftKey && !e.altKey && !e.ctrlKey) {
          if (!disabled) {
            onSubmitQuery((e as any).currentTarget.value)
          }
          e.preventDefault()
          return
        }
        if (e.shiftKey) {
          return 'Enter'
        }
        e.preventDefault()
      }
    }
</script>

<script lang="ts">
  import SqlValue from '$lib/components/relationData/SQLValue.svelte'
  import SqlColumnHeader from '$lib/components/relationData/SQLColumnHeader.svelte'
  import { usePopoverTooltip } from '$lib/compositions/common/usePopoverTooltip.svelte'
  import ReverseScrollFixedList from '$lib/components/pipelines/editor/ReverseScrollFixedList.svelte'

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
</script>

<div
  class="bg-white-black absolute m-0 w-max max-w-lg -translate-x-[4.5px] -translate-y-[2.5px] whitespace-break-spaces break-words border border-surface-500 px-2 py-1 text-surface-950-50"
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
    <div class="flex max-w-[1000px] flex-col">
      <div class="flex w-full flex-nowrap">
        <textarea
          bind:value={query}
          style="font-family: {theme.config.monospaceFontFamily}; field-sizing: content"
          class="bg-white-black !border-1 w-full overflow-auto !border-l-4 !border-surface-500 !ring-0 !ring-primary-500 text-surface-950-50 scrollbar focus:!border-primary-500"
          placeholder="SELECT * FROM ..."
          onkeydown={handleKeyDown(onSubmitQuery, disabled)}
        ></textarea>

        <div class="flex h-10 flex-none">
          {#if progress}
            <button
              class="fd fd-stop w-10 p-2 text-[24px]"
              onclick={onCancelQuery}
              aria-label="Stop query"
            ></button>
          {:else}
            <button
              {disabled}
              class="fd fd-play_arrow -ml-1 -mt-1 mb-1 mr-1 w-10 p-2 text-[32px]"
              onclick={() => onSubmitQuery(query)}
              aria-label="Run query"
            ></button>
          {/if}
          {#if !isLastQuery}
            <button
              class="fd fd-delete w-10 p-2 text-[24px]"
              onclick={onDeleteQuery}
              aria-label="Delete query"
            ></button>
          {:else}
            <div class="w-10"></div>
          {/if}
        </div>
      </div>
      <div class="flex h-6 flex-nowrap items-center gap-4 whitespace-nowrap">
        {#if result}
          {result.rows.length > 1
            ? `${result.rows.length} rows`
            : result.rows.length === 0
              ? 'No rows returned'
              : ''}
        {/if}
        {#if progress}
          <Progress value={null} meterBg="bg-primary-500" base="pr-20 h-1 max-w-[1000px]"
          ></Progress>
        {/if}
      </div>
    </div>

    {#if result}
      <div class="relative mr-4">
        <ReverseScrollFixedList
          itemSize={28}
          items={result.rows}
          class="overflow-scroll scrollbar"
          stickyIndices={[]}
          marginTop={28}
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
              class="h-full max-h-64 max-w-full overflow-auto scrollbar"
              {onscroll}
              bind:clientHeight={_height.current}
              bind:this={ref.current}
            >
              <table style:height>
                {#if result.columns.length}
                  <thead class="bg-white-black sticky top-0 z-10 !mb-0 h-7">
                    <tr>
                      {#each result.columns as column}
                        <SqlColumnHeader {column}></SqlColumnHeader>
                      {/each}
                    </tr>
                  </thead>
                {/if}
                <tbody>
                  {@render children()}
                </tbody>
              </table>
            </div>
          {/snippet}
          {#snippet item(row, style, padding, isSticky)}
            {#if 'cells' in row}
              <tr {style} class="h-7 whitespace-nowrap even:bg-surface-50-950">
                {#each row.cells as value}
                  <SqlValue
                    {value}
                    props={(format) => ({
                      onclick: tooltip.showTooltip(format(value)),
                      onmouseleave: tooltip.onmouseleave
                    })}
                  ></SqlValue>
                {/each}
              </tr>
            {:else if 'error' in row}
              <tr {style} class="h-7">
                <td colspan="99999999" class="px-2 preset-tonal-error">{row.error}</td>
              </tr>
            {:else}
              <tr {style} class="h-7">
                <td colspan="99999999" class="px-2 preset-tonal-warning">{row.warning}</td>
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
    {/if}
  </div>
</div>
