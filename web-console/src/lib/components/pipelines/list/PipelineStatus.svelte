<script lang="ts">
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import { type PipelineStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()
  const chipClass = $derived(
    match(status)
      .with('Shutdown', () => 'preset-tonal-success')
      .with('Starting up', () => 'preset-tonal-tertiary')
      .with('Initializing', () => 'preset-tonal-warning')
      .with('Paused', () => 'preset-tonal-success')
      .with('Running', () => 'preset-tonal-success')
      .with('ShuttingDown', () => 'preset-tonal-tertiary')
      .with({ PipelineError: P.any }, () => 'preset-tonal-error')
      .with('Compiling sql', () => 'preset-tonal-warning')
      .with('Queued', () => 'preset-tonal-warning')
      .with('Compiling bin', () => 'preset-tonal-warning')
      .with(
        { SqlError: P.any },
        { RustError: P.any },
        { SystemError: P.any },
        () => 'preset-tonal-error'
      )
      .exhaustive()
  )
</script>

<div
  class={'w-30 chip pointer-events-none h-6 flex-none text-[0.66rem] uppercase ' +
    chipClass +
    ' ' +
    _class}
>
  {getPipelineStatusLabel(status)}
</div>
