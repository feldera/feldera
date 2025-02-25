<script lang="ts">
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import { type PipelineStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()
  const chipClass = $derived(
    match(status)
      .with('Shutdown', { SqlWarning: P.any }, () => 'bg-surface-100-900')
      .with('Preparing', 'Provisioning', 'Initializing', () => 'preset-filled-tertiary-200-800')
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
</script>

<div class={'chip pointer-events-none w-32 uppercase ' + chipClass + ' ' + _class}>
  {getPipelineStatusLabel(status)}
</div>
