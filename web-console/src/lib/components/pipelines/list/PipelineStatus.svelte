<script lang="ts">
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import { type PipelineStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()
  const chipClass = $derived(
    match(status)
      .with('Stopped', () => 'bg-surface-100-900')
      .with('Preparing', 'Provisioning', 'Initializing', () => 'preset-filled-tertiary-200-800')
      .with('Paused', () => 'bg-blue-200 dark:bg-blue-800')
      .with('Suspending', () => 'bg-blue-200 dark:bg-blue-800')
      .with('Running', () => 'preset-filled-success-200-800')
      .with('Pausing', () => 'preset-filled-secondary-200-800')
      .with('Resuming', () => 'preset-filled-tertiary-200-800')
      .with('Stopping', () => 'preset-filled-secondary-200-800')
      .with(
        { Queued: P.any },
        { CompilingSql: P.any },
        { SqlCompiled: P.any },
        { CompilingRust: P.any },
        () => 'preset-filled-warning-200-800'
      )
      .with('Unavailable', () => 'bg-orange-200 dark:bg-orange-800')
      .with(
        'SqlError',
        'RustError',
        'SystemError',
        () => 'preset-filled-error-50-950'
      )
      .exhaustive()
  )
</script>

<div class={'chip pointer-events-none w-32 uppercase ' + chipClass + ' ' + _class}>
  {getPipelineStatusLabel(status)}
</div>
