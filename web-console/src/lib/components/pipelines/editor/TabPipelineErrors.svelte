<script lang="ts">
  import { useSystemErrors } from '$lib/compositions/health/systemErrors.svelte'
  import type { ExtendedPipeline, Pipeline } from '$lib/services/pipelineManager'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()

  let errors = useSystemErrors(pipeline)
</script>

<div class="flex h-full flex-col gap-4 p-2">
  {#each errors.current as systemError}
    <div class="whitespace-nowrap">
      <a href={systemError.cause.source}>
        <span class=" bx bx-x-circle text-[20px] text-error-500"></span></a
      >
      <span class=" whitespace-pre-wrap break-words align-text-bottom font-mono">
        {systemError.message}
      </span>
    </div>
  {:else}
    <span class="text-surface-600-400">No errors</span>
  {/each}
</div>
