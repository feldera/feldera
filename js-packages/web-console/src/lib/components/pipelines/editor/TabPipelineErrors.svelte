<script lang="ts">
  import { selectScope } from 'common-ui'
  import { extractProgramStderr, type SystemError } from '#lib/compositions/health/systemErrors.ts'
  import { useLayoutSettings } from '#lib/compositions/layout/useLayoutSettings.svelte.ts'
  import { useSkeletonTheme } from '#lib/compositions/useSkeletonTheme.svelte.ts'
  import type { ExtendedPipeline } from '#lib/services/pipelineManager.ts'
  import { useDarkMode } from '#lib/compositions/useDarkMode.svelte.ts'
  import AllClearLight from '#assets/illustrations/all-clear-light.svg?component'
  import AllClearDark from '#assets/illustrations/all-clear-dark.svg?component'

  const { hideWarnings, verbatimErrors } = useLayoutSettings()

  const {
    pipeline,
    errors
  }: {
    pipeline: { current: ExtendedPipeline }
    errors: SystemError<any, any>[]
  } = $props()

  const theme = useSkeletonTheme()
  const darkMode = useDarkMode()
</script>

{#snippet noErrors(text: string)}
  <!-- The viewBox crops the drawing's empty margins so the label sits close to it -->
  <div class="flex flex-1 flex-col items-center justify-center gap-3 p-2 text-center">
    {#if darkMode.current === 'dark'}
      <AllClearDark viewBox="60 55 120 64" class="h-[51px] w-[96px]"></AllClearDark>
    {:else}
      <AllClearLight viewBox="60 55 120 64" class="h-[51px] w-[96px]"></AllClearLight>
    {/if}
    <span class="text-surface-600-400">{text}</span>
  </div>
{/snippet}

<div class="scrollbar h-full w-full overflow-y-auto">
  <div
    class="flex min-h-full w-fit min-w-full flex-col gap-4 rounded pt-2"
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
        {@render noErrors('No errors')}
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
        {@render noErrors(
          hideWarnings.value && errors.length ? 'No errors, warnings hidden' : 'No errors'
        )}
      {/each}
    {/if}
  </div>
</div>
