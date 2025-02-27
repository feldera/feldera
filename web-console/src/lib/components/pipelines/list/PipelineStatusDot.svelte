<script lang="ts">
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import { type PipelineStatus } from '$lib/services/pipelineManager'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { match, P } from 'ts-pattern'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()
  const chipClass = $derived(
    match(status)
      .with('Shutdown', () => 'preset-filled-surface-400-600')
      .with('Preparing', 'Provisioning', 'Initializing', () => 'preset-filled-tertiary-200-800')
      .with('Paused', () => 'preset-filled-warning-400-600')
      .with('Running', () => 'preset-filled-success-400-600')
      .with('Pausing', () => 'preset-filled-secondary-200-800')
      .with('Resuming', () => 'preset-filled-tertiary-200-800')
      .with('ShuttingDown', () => 'preset-filled-secondary-200-800')
      .with({ PipelineError: P.any }, () => 'preset-filled-error-400-600')
      .with(
        { Queued: P.any },
        { CompilingSql: P.any },
        { SqlCompiled: P.any },
        { CompilingRust: P.any },
        () => 'preset-filled-warning-400-600'
      )
      .with('Unavailable', () => 'bg-orange-300 dark:bg-orange-700')
      .with('SqlError', 'RustError', 'SystemError', () => 'preset-filled-error-400-600')
      .exhaustive()
  )
</script>

<div class="p-2">
  <div class="h-3 w-3 flex-none rounded-full text-[0.66rem] uppercase {chipClass} {_class}"></div>
</div>
<Tooltip
  class="pointer-events-none ml-2 whitespace-nowrap rounded bg-white text-surface-950-50 dark:bg-black"
  placement="left">{getPipelineStatusLabel(status)}</Tooltip
>
