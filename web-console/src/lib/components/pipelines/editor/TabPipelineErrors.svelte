<script lang="ts">
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import {
    extractPipelineStderr,
    extractPipelineXgressStderr,
    extractProgramStderr,
    type SystemError
  } from '$lib/compositions/health/systemErrors'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { Switch } from '@skeletonlabs/skeleton-svelte'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import { selectScope } from '$lib/compositions/common/userSelect'

  let { hideWarnings, verbatimErrors } = useLayoutSettings()

  let {
    pipeline,
    errors,
    metrics
  }: {
    pipeline: { current: ExtendedPipeline }
    errors: SystemError<any, any>[]
    metrics: { current: PipelineMetrics }
  } = $props()

  const theme = useSkeletonTheme()
</script>

<div
  class="flex w-full flex-nowrap justify-between gap-6 py-2 sm:pt-0 lg:absolute lg:right-0 lg:-mt-12 lg:w-auto"
>
  <label class="flex cursor-pointer items-center gap-2" class:disabled={verbatimErrors.value}>
    Hide warnings
    <input class="checkbox" type="checkbox" bind:checked={hideWarnings.value} />
  </label>
  <label class="flex cursor-pointer items-center justify-end gap-2 rounded lg:justify-normal">
    Verbatim errors
    <Switch name="verbatimErrors" bind:checked={verbatimErrors.value}></Switch>
  </label>
</div>
<div class="flex h-full overflow-y-auto scrollbar">
  <div
    class="flex min-h-full flex-1 flex-col gap-4 rounded"
    use:selectScope
    role="textbox"
    tabindex={99}
  >
    {#if verbatimErrors.value}
      {@const stderr = [
        ...extractProgramStderr(pipeline.current),
        ...extractPipelineXgressStderr({
          pipelineName: pipeline.current.name,
          status: metrics.current
        }),
        ...extractPipelineStderr(pipeline.current)
      ].join('\n')}
      {#if stderr}
        <div
          class="bg-white-dark flex flex-1 whitespace-pre-wrap rounded p-4"
          style="font-family: {theme.config.monospaceFontFamily}"
        >
          {stderr}
        </div>
      {:else}
        <span class="text-surface-600-400">No errors</span>
      {/if}
    {:else}
      {#each hideWarnings.value ? errors.filter((e) => !e.cause.warning) : errors as systemError}
        <div class="bg-white-dark whitespace-nowrap rounded p-4">
          <a
            href={systemError.cause.source}
            aria-label={systemError.cause.warning ? 'Warning location' : 'Error location'}
          >
            <span
              class="pr-2 text-[20px] {systemError.cause.warning
                ? 'fd fd-triangle-alert text-warning-500'
                : 'fd fd-circle-x text-error-500'}"
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
    {/if}
  </div>
</div>
