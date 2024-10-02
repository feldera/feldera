<script lang="ts">
  import { useSystemErrors } from '$lib/compositions/health/systemErrors.svelte'
  import type { ExtendedPipeline, Pipeline } from '$lib/services/pipelineManager'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()

  let errors = useSystemErrors(pipeline)

  const theme = useSkeletonTheme()
</script>

<div class=" h-full">
  <div class="{errors.current.length ? 'bg-white-black' : ''} flex min-h-full flex-col p-2">
    {#each errors.current as systemError}
      <div class=" whitespace-nowrap">
        <a class="" href={systemError.cause.source}>
          <span
            class=" text-[20px] {systemError.cause.warning
              ? 'fd fd-warning_amber text-warning-500'
              : 'fd fd-close_circle_outline text-error-500'}"
          >
          </span></a
        >
        <span
          class="whitespace-pre-wrap break-words text-start align-text-top leading-none"
          style="font-family: {theme.config.monospaceFontFamily}"
        >
          {systemError.message}
        </span>
      </div>
    {:else}
      <span class="text-surface-600-400">No errors</span>
    {/each}
  </div>
</div>
