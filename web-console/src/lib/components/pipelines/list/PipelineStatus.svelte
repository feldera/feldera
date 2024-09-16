<script lang="ts">
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import { type PipelineStatus } from '$lib/services/pipelineManager'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { match, P } from 'ts-pattern'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()
  const chipClass = $derived(
    match(status)
      .with('Shutdown', () => 'preset-filled-surface-400-600')
      .with('Starting up', () => 'preset-filled-primary-300-700')
      .with('Initializing', () => 'preset-filled-primary-300-700')
      .with('Paused', () => 'preset-filled-warning-400-600')
      .with('Running', () => 'preset-filled-success-400-600')
      .with('Pausing', () => 'preset-filled-primary-300-700')
      .with('Resuming', () => 'preset-filled-primary-300-700')
      .with('ShuttingDown', () => 'preset-filled-primary-300-700')
      .with({ PipelineError: P.any }, () => 'preset-filled-error-400-600')
      .with('Compiling sql', () => 'preset-filled-warning-400-600')
      .with('Queued', () => 'preset-filled-warning-400-600')
      .with('Compiling bin', () => 'preset-filled-warning-400-600')
      .with(
        { SqlError: P.any },
        { RustError: P.any },
        { SystemError: P.any },
        () => 'preset-filled-error-400-600'
      )
      .exhaustive()
  )
</script>

<div class="p-2">
  <div class="h-4 w-4 flex-none rounded-full text-[0.66rem] uppercase {chipClass} {_class}"></div>
</div>
<Tooltip class="bg-white text-surface-950-50 dark:bg-black" placement="left"
  >{getPipelineStatusLabel(status)}</Tooltip
>
