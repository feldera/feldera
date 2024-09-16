<script lang="ts">
  import { useSystemErrors } from '$lib/compositions/health/systemErrors.svelte'
  import type { ExtendedPipeline, Pipeline } from '$lib/services/pipelineManager'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()

  let errors = useSystemErrors(pipeline)
  const theme = useSkeletonTheme()
</script>

<div class="flex h-full flex-col gap-4 p-2">
  {#each errors.current as systemError}
    <div class="whitespace-nowrap">
      <a href={systemError.cause.source}>
        <span
          class=" text-[20px] {systemError.cause.warning
            ? 'bx bx-error text-warning-500'
            : 'bx bx-x-circle text-error-500'}"
        ></span></a
      >
      <span
        class="whitespace-pre-wrap break-words align-text-bottom"
        style="font-family: {theme.config.monospaceFontFamily}"
      >
        {systemError.message}
      </span>
    </div>
  {:else}
    <span class="text-surface-600-400">No errors</span>
  {/each}
</div>
