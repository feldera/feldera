<script lang="ts">
  import { asyncDerived, derived, readable, writable } from '@square/svelte-store'
  import { useSystemErrors } from '$lib/compositions/health/systemErrors'

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
    <div class="whitespace-nowrap">
      <a href={systemError.cause.source}>
        <span class=" bx bx-x-circle text-error-500 text-[20px]"></span></a>
      <span class=" whitespace-pre-wrap break-words align-text-bottom">
        {systemError.message}
      </span>
    </div>
  {:else}
    <span class="text-surface-600-400">No errors</span>
  {/each}
</div>
