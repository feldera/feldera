<script lang="ts">
  import { getDeploymentStatusLabel } from '$lib/functions/pipelines/status'
  import { type PipelineStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()

  const chipClass = $derived(
    match(status)
      .with('Shutdown', () => '')
      .with('Starting up', () => 'preset-tonal-primary')
      .with('Initializing', () => 'preset-tonal-primary')
      .with('Paused', () => 'preset-tonal-warning')
      .with('Running', () => 'preset-tonal-success')
      .with('Pausing', () => 'preset-tonal-primary')
      .with('Resuming', () => 'preset-tonal-primary')
      .with('ShuttingDown', () => 'preset-tonal-primary')
      .with({ PipelineError: P.any }, () => '')
      .with('Compiling sql', () => '')
      .with('Queued', () => '')
      .with('Compiling bin', () => '')
      .with({ SqlError: P.any }, { RustError: P.any }, { SystemError: P.any }, () => '')
      .exhaustive()
  )
</script>

<div
  class={'w-30 chip pointer-events-none h-6 flex-none text-[0.66rem] uppercase ' +
    chipClass +
    ' ' +
    _class}
>
  {getDeploymentStatusLabel(status)}
</div>
