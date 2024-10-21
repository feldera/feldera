<script lang="ts" module>
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { getCaseIndependentName } from '$lib/functions/felderaRelation'
  import type { JSONXgressValue, SQLValueJS } from '$lib/functions/sqlValue'
  import type { Field } from '$lib/services/manager'
  import type { XgressRecord } from '$lib/types/pipelineManager'
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import BigNumber from 'bignumber.js'
  import JSONbig from 'true-json-bigint'
  import { match, P } from 'ts-pattern'

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
  let cellDetails: { ref: HTMLElement; value: any } | undefined = $state()
  let popupData: { x: number; y: number; text: string } | undefined = $state()
  type CustomMouseEvent = MouseEvent & {
    currentTarget: EventTarget & HTMLTableCellElement
  }
  const handleCellHoverImpl = (e: CustomMouseEvent, value: any) => {
    cellDetails = { ref: e.currentTarget, value }
  }
  const handleCellHover = (value: any) => (e: CustomMouseEvent) => handleCellHoverImpl(e, value)
  const handleCellMouseLeave = (e: CustomMouseEvent) => {
    cellDetails = undefined
  }
  $effect(() => {
    if (!popupRef) {
      return
    }
    if (!cellDetails) {
      popupData = undefined
      popupRef.hidePopover()
      return
    }
    const text = cellDetails.value
    const rect = cellDetails.ref.getBoundingClientRect()
    popupData = {
      x: 0,
      y: 0,
      text
    }
    popupRef.showPopover()
    requestAnimationFrame(() => {
      const rect2 = popupRef!.getBoundingClientRect()
      popupData = {
        x: Math.min(Math.max(0, rect.left), window.innerWidth - rect2.width),
        y: Math.min(rect.top, window.innerHeight - rect2.height),
        text
      }
    })
  })
</script>

<div
  class="bg-white-black pointer-events-none absolute m-0 w-max max-w-lg whitespace-break-spaces break-words border border-surface-500 px-2 py-1 text-surface-950-50"
  popover="manual"
  bind:this={popupRef}
  style={popupData ? `left: ${popupData.x}px; top: ${popupData.y}px` : ''}
>
  {popupData?.text}
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
    <div class="flex max-w-[1000px] flex-nowrap">
      <textarea
        rows={3}
        bind:value={query}
        style="font-family: {theme.config.monospaceFontFamily}; field-sizing: content"
        class="bg-white-black !border-1 w-full overflow-auto !border-l-4 !border-surface-500 !ring-0 !ring-primary-500 text-surface-950-50 focus:!border-primary-500"
        placeholder="SELECT * FROM ..."
        onkeydown={handleKeyDown(onSubmitQuery, disabled)}
      ></textarea>

      <div class="flex w-10 flex-col gap-2">
        {#if progress}
          <button
            class="fd fd-stop w-10 p-2 text-[24px]"
            onclick={onCancelQuery}
            aria-label="Stop query"
          ></button>
        {:else}
          <button
            {disabled}
            class="fd fd-play_arrow -ml-1 -mt-1 mb-1 h-10 w-12 p-2 text-[32px]"
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
        {/if}
      </div>
    </div>
    {#if progress}
      <Progress value={null} meterBg="bg-primary-500" base="py-2 h-5 -mb-5 max-w-[1000px] pr-10"
      ></Progress>
    {/if}
    {#if result}
      <div class="mt-5">
        {result.rows.length > 1 ? `${result.rows.length} rows` : ''}
      </div>
      <div class="mr-4 max-h-64 w-fit max-w-full overflow-auto">
        <table class=" border-separate border-spacing-x-1">
          {#if result.columns.length}
            <thead class="sticky top-0 !mb-0 bg-surface-50-950">
              <tr>
                {#each result.columns as column}
                  <th>{getCaseIndependentName(column)}</th>
                {/each}
              </tr>
            </thead>
          {/if}
          <tbody>
            {#each result.rows as row}
              {#if 'cells' in row}
                <tr class="whitespace-nowrap even:bg-surface-50-950">
                  {#each row.cells as value}
                    {@const text =
                      typeof value === 'string' ? value : JSONbig.stringify(value, undefined, 1)}
                    <td onmouseenter={handleCellHover(text)} onmouseleave={handleCellMouseLeave}>
                      {text.slice(0, 37)}{text.length > 36 ? '...' : ''}
                    </td>
                  {/each}
                </tr>
              {:else if 'error' in row}
                <tr>
                  <td colspan="99999999" class="px-2 preset-tonal-error">{row.error}</td>
                </tr>
              {:else}
                <tr>
                  <td colspan="99999999" class="px-2 preset-tonal-warning">{row.warning}</td>
                </tr>
              {/if}
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>
</div>
