<script lang="ts" module>
  import { getCaseIndependentName } from '$lib/functions/felderaRelation'
  import type { SQLValueJS } from '$lib/functions/sqlValue'
  import type { Field } from '$lib/services/manager'
  import { Progress } from '@skeletonlabs/skeleton-svelte'

  export type Row = { cells: SQLValueJS[] } | { error: string }

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
      if (e.key === 'Enter') {
        // Enter pressed
        //No shift or alt or ctrl
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
    // value
    query = $bindable(),
    result,
    progress,
    onSubmitQuery,
    onDeleteQuery,
    disabled,
    isLastQuery
  }: {
    // value: QueryData
    onSubmitQuery: (query: string) => void
    onDeleteQuery: () => void
    disabled: boolean
    isLastQuery: boolean
  } & QueryData = $props()

  $effect(() => {
    // console.log('effect', JSON.stringify(progress))
  })
</script>

<div class="mr-4 flex flex-nowrap items-start">
  <div class="w-full">
    <textarea
      bind:value={query}
      class="textarea bg-white dark:bg-black"
      placeholder="SQL"
      onkeydown={handleKeyDown(onSubmitQuery, disabled)}></textarea>
    {#if progress}
      <Progress value={null} meterBg="bg-secondary-500" base="py-2 h-5 -mb-5"></Progress>
    {/if}
    {#if result}
      <div class="mt-5 max-h-64 overflow-auto">
        <table>
          {#if result.columns.length}
            <thead class="bg-surface-50-950 sticky top-0 !mb-0 border-b-2">
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
                <tr class="even:bg-white even:dark:bg-black">
                  {#each row.cells as value}
                    <td>
                      {String(value)}
                    </td>
                  {/each}
                </tr>
              {:else}
                <tr>
                  <td colspan="99999999" class="preset-filled-error-50-950 px-2">{row.error}</td>
                </tr>
              {/if}
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>
  <button
    class="{isLastQuery ? 'pointer-events-none' : 'fd fd-delete'} w-10 p-2 text-[24px]"
    onclick={onDeleteQuery}
    aria-label="Delete query"></button>
</div>
