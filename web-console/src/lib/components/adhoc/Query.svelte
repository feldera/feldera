<script lang="ts" module>
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { getCaseIndependentName } from '$lib/functions/felderaRelation'
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
</script>

<div
  class="mr-4 flex flex-nowrap items-start"
  oncopy={(e) => {
    if (window.getSelection()?.toString().length) {
      return
    }
    e.preventDefault()
    onCancelQuery?.()
  }}
>
  <div class="w-full max-w-[1000px]">
    <textarea
      rows={3}
      bind:value={query}
      style="font-family: {theme.config.monospaceFontFamily}"
      class="bg-white-black w-full overflow-auto !border-0 !border-l-4 !border-surface-500 !ring-0 !ring-primary-500 text-surface-950-50 focus:!border-primary-500"
      placeholder="SELECT * FROM ..."
      onkeydown={handleKeyDown(onSubmitQuery, disabled)}
    ></textarea>
    {#if progress}
      <Progress value={null} meterBg="bg-primary-500" base="py-2 h-5 -mb-5"></Progress>
    {/if}
    {#if result}
      <div class="mr-auto mt-5 max-h-64 w-fit overflow-auto">
        <table class="">
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
                <tr class="even:bg-surface-50-950">
                  {#each row.cells as value}
                    <td>
                      {String(value)}
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
  <div class="flex w-10 flex-col gap-2">
    {#if progress}
      <button
        class="fd fd-stop w-10 p-2 text-[24px]"
        onclick={onCancelQuery}
        aria-label="Stop query"
      ></button>
    {:else}
      <button
        class="fd fd-play_arrow w-10 p-2 text-[24px]"
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
