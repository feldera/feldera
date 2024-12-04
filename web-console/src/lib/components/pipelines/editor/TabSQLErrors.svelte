<script lang="ts">
  import { useSqlErrors } from '$lib/compositions/health/systemErrors'
  import { groupBy } from '$lib/functions/common/array'
  import { tuple } from '$lib/functions/common/tuple'
  import { asyncDerived, derived, readable, writable } from '@square/svelte-store'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'

  let { pipelineName }: { pipelineName: string } = $props()

  const sqlErrors = useSqlErrors()
  const _pipelineName = writable(pipelineName)
  $effect(() => {
    $_pipelineName = pipelineName
  })
  const programsErrors = derived([sqlErrors, _pipelineName], ([errors, pipelineName]) => {
    const es = errors.filter((error) => error.cause.entityName === pipelineName)
    return es.length ? [tuple(pipelineName, es)] : []
  })
</script>

<div class="flex flex-col">
  {#each $programsErrors as [pipelineName, errors]}
    <a href={errors[0].cause.source}>
      {pipelineName} <span class="text-surface-600-400">program</span>
    </a>
    {#each errors as error}
      <a
        href={error.cause.source}
        class="relative overflow-hidden text-ellipsis whitespace-nowrap pl-8"
      >
        <span class="fd fd-circle-x text-error-500"></span>
        <span class="absolute">{error.cause.body?.message ?? error.cause.body}</span>
      </a>
      <Tooltip
        activeContent
        class=" max-h-64 max-w-full overflow-y-auto whitespace-break-spaces rounded bg-white p-2 shadow-md text-surface-950-50 dark:bg-black"
      >
        {error.cause.body?.message ?? error.cause.body}
      </Tooltip>
    {/each}
  {:else}
    <span class="text-surface-600-400">No SQL errors</span>
  {/each}
</div>
