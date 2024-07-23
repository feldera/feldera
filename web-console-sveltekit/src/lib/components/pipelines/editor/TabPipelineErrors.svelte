<script lang="ts">
  import {} from '$lib/compositions/health/systemErrors'
  import { groupBy } from '$lib/functions/common/array'
  import { tuple } from '$lib/functions/common/tuple'
  import { asyncDerived, derived, readable, writable } from '@square/svelte-store'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { useSystemErrors } from '$lib/compositions/health/systemErrors'
  import ErrorTile from '$lib/components/health/ErrorTile.svelte'

  let { pipelineName }: { pipelineName: string } = $props()
  let _pipelineName = writable(pipelineName)
  $effect(() => {
    $_pipelineName = pipelineName
  })
  let allErrors = useSystemErrors()
  let errors = derived([allErrors, _pipelineName], ([errors, pipelineName]) =>
    errors.filter((err) => err.cause.entityName === pipelineName)
  )
</script>

<div class="flex h-full flex-col gap-4">
  {#each $errors as systemError}
    <div class="">
      <!-- <ErrorTile {systemError}>
        {#snippet before()}
          <span class="bx bx-x-circle text-error-500"></span>
        {/snippet}
      </ErrorTile> -->
      <span class="bx bx-x-circle text-error-500"></span>
      <span>
        {systemError.message}
      </span>
    </div>
  {:else}
    <span class="text-surface-600-400">No errors</span>
  {/each}
</div>
