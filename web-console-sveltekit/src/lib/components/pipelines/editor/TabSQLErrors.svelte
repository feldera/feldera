<script lang="ts">
  import { useSqlErrors } from '$lib/compositions/health/systemErrors'
  import { groupBy } from '$lib/functions/common/array'
  import { derived } from '@square/svelte-store'
  import Tooltip from 'sv-tooltip'

  const sqlErrors = useSqlErrors()
  console.log('tab sqlErrors', $sqlErrors)
  const programsErrors = derived(sqlErrors, (errors) =>
    groupBy(errors, (error) => error.cause.entityName)
  )
</script>

<div class="flex flex-col">
  {#each $programsErrors as [pipelineName, errors]}
    <a href={errors[0].cause.source}>
      {pipelineName} <span class="text-surface-600-400">program</span>
    </a>
    {#each errors as error}
      <Tooltip color="" top>
        <a
          href={error.cause.source}
          class="block overflow-hidden text-ellipsis whitespace-nowrap pl-8">
          <span class="bx bx-x-circle text-error-500"></span>
          {error.cause.body.message}
        </a>
        <div
          slot="custom-tip"
          class=" text-surface-950-50 max-h-64 max-w-full overflow-y-auto whitespace-break-spaces rounded bg-white p-2 shadow-md dark:bg-black">
          {error.cause.body.message}
        </div>
      </Tooltip>
    {/each}
  {/each}
</div>
