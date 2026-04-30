<script lang="ts">
  import { selectScope } from '$lib/compositions/common/userSelect'
  import { extractProgramStderr, type SystemError } from '$lib/compositions/health/systemErrors'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'

  const { hideWarnings, verbatimErrors } = useLayoutSettings()

  const {
    pipeline,
    errors
  }: {
    pipeline: { current: ExtendedPipeline }
    errors: SystemError<any, any>[]
  } = $props()

  const theme = useSkeletonTheme()
</script>

<div class="scrollbar h-full w-full overflow-y-auto">
  <div
    class="flex min-h-full w-fit min-w-full flex-col gap-4 rounded"
    use:selectScope
    role="textbox"
    tabindex={-1}
  >
    {#if verbatimErrors.value}
      {@const stderr = [...extractProgramStderr(pipeline.current)].join('\n')}
      {#if stderr}
        <div
          class="bg-white-dark flex flex-1 rounded p-4 whitespace-pre-wrap"
          style="font-family: {theme.config.monospaceFontFamily}"
        >
          {stderr}
        </div>
      {:else}
        <span class="text-surface-600-400">No errors</span>
      {/if}
    {:else}
      {#each hideWarnings.value ? errors.filter((e) => !e.cause.warning) : errors as systemError}
        <div class="bg-white-dark rounded p-4 break-all whitespace-nowrap">
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
            class="text-start align-text-top leading-none wrap-break-word whitespace-pre-wrap"
            style="font-family: {theme.config.monospaceFontFamily}"
          >
            {systemError.message}
          </span>
        </div>
      {:else}
        <span class="text-surface-600-400">
          {#if hideWarnings.value && errors.length}
            No errors, warnings hidden
          {:else}
            No errors
          {/if}
        </span>
      {/each}
    {/if}
  </div>
</div>
