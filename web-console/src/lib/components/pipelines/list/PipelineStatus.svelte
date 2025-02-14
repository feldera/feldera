<script lang="ts">
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import { type PipelineStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'

  const {
    status,
    statusText,
    class: _class = ''
  }: { status: PipelineStatus; class?: string; statusText?: string | null } = $props()
  const chipClass = $derived(
    match(status)
      .with('Shutdown', { SqlWarning: P.any }, () => 'bg-surface-100-900')
      .with('Starting up', () => 'preset-filled-tertiary-200-800')
      .with('Initializing', () => 'preset-filled-tertiary-200-800')
      .with('Paused', () => 'preset-filled-warning-200-800')
      .with('Running', () => 'preset-filled-success-200-800')
      .with('Pausing', () => 'preset-filled-secondary-200-800')
      .with('Resuming', () => 'preset-filled-tertiary-200-800')
      .with('ShuttingDown', () => 'preset-filled-secondary-200-800')
      .with(
        { Queued: P.any },
        { 'Compiling SQL': P.any },
        { 'SQL compiled': P.any },
        { 'Compiling binary': P.any },
        () => 'preset-filled-warning-200-800'
      )
      .with('Unavailable', () => 'bg-orange-200 dark:bg-orange-800')
      .with(
        { PipelineError: P.any },
        { SqlError: P.any },
        { RustError: P.any },
        { SystemError: P.any },
        () => 'preset-filled-error-50-950'
      )
      .exhaustive()
  )

  const theme = useSkeletonTheme()
</script>

<div class={'chip w-32 uppercase hover:brightness-100 ' + chipClass + ' ' + _class}>
  {getPipelineStatusLabel(status)}
</div>
{#if statusText}
  <Tooltip
    activeContent
    class="bg-white-dark z-10 whitespace-pre-wrap rounded p-4 text-base max-w-[1000px]"
    style="font-family: {theme.config.monospaceFontFamily}"
  >
    {statusText}
  </Tooltip>
{/if}
