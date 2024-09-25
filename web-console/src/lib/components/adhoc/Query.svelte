<script lang="ts" module>
  import { getCaseIndependentName } from '$lib/functions/felderaRelation'
  import type { SQLValueJS } from '$lib/functions/sqlValue'
  import type { Field } from '$lib/services/manager'
  import { Progress } from '@skeletonlabs/skeleton-svelte'

  export type QueryData = {
    query: string
    result?: Promise<{ rows: SQLValueJS[][]; columns: Field[] } | Error>
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
    value = $bindable(),
    onSubmitQuery,
    onDeleteQuery,
    disabled,
    isLastQuery
  }: {
    value: QueryData
    onSubmitQuery: (query: string) => void
    onDeleteQuery: () => void
    disabled: boolean
    isLastQuery: boolean
  } = $props()
</script>

<div class="mr-4 flex flex-nowrap items-start">
  <div class="w-full">
    <textarea
      bind:value={value.query}
      class="textarea bg-white dark:bg-black"
      placeholder="SQL"
      onkeydown={handleKeyDown(onSubmitQuery, disabled)}
    ></textarea>
    {#if value.result}
      {#await value.result}
        <Progress value={null} meterBg="bg-secondary-500" base="py-2 h-5"></Progress>
      {:then result}
        {#if 'rows' in result}
          <div class="my-2 max-h-64 overflow-auto">
            <table>
              <thead class="sticky top-0 !mb-0 border-b-2 bg-surface-50-950">
                <tr>
                  {#each result.columns as column}
                    <th>{getCaseIndependentName(column)}</th>
                  {/each}
                </tr>
              </thead>
              <tbody>
                {#each result.rows as row}
                  <tr class="even:bg-white even:dark:bg-black">
                    {#each row as value}
                      <td>
                        {String(value)}
                      </td>
                    {/each}
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {:else}
          <div class="whitespace-pre-wrap pt-1 font-mono text-error-500">
            {result.details.error}
          </div>
        {/if}
      {/await}
    {/if}
  </div>
  <button
    class="{isLastQuery ? '' : 'fd fd-delete'}  w-10 p-2 text-[24px]"
    onclick={onDeleteQuery}
    aria-label="Delete query"
  ></button>
</div>
