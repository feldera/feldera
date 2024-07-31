<script lang="ts">
  import { getStatusLabel } from '$lib/functions/pipelines/status'
  import { getPipelineStatus, type PipelineStatus } from '$lib/services/pipelineManager'
  import { asyncDerived, asyncReadable, readable } from '@square/svelte-store'
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
  class={'chip pointer-events-none h-6 w-20 flex-none text-[0.66rem] uppercase ' +
    chipClass +
    ' ' +
    _class}
>
  {getStatusLabel(status)}
</div>
<!-- {#each ['preset-tonal-primary', 'preset-tonal-secondary', 'preset-tonal-tertiary', 'preset-tonal-success', 'preset-tonal-warning', 'preset-tonal-error', 'preset-tonal-surface'] as color}
  <div class={'chip ' + color}>fffffffff</div>
{/each} -->
