<script lang="ts">
  import { getDeploymentStatusLabel } from '$lib/functions/pipelines/status'
  import { type PipelineStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()

  const chipClass = $derived(
    match(status)
      .with('Shutdown', () => '')
      .with('Starting up', () => 'preset-filled-tertiary-200-800')
      .with('Initializing', () => 'preset-filled-tertiary-200-800')
      .with('Paused', () => 'preset-tonal-warning')
      .with('Running', () => 'preset-tonal-success')
      .with('Pausing', () => 'preset-filled-secondary-200-800')
      .with('Resuming', () => 'preset-filled-tertiary-200-800')
      .with('ShuttingDown', () => 'preset-filled-secondary-200-800')
      .with({ PipelineError: P.any }, () => '')
      .with('Compiling sql', () => '')
      .with('Queued', () => '')
      .with('Compiling bin', () => '')
      .with({ SqlError: P.any }, { RustError: P.any }, { SystemError: P.any }, () => '')
      .exhaustive()
  )
</script>

<div class={'w-30 chip pointer-events-none text-[0.66rem] uppercase ' + chipClass + ' ' + _class}>
  {getDeploymentStatusLabel(status)}
</div>
