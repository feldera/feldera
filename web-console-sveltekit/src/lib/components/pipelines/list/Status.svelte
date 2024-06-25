<script lang="ts">
  import { getPipelineStatus } from '$lib/services/pipelineManager'
  import { asyncDerived, derived, readable } from '@square/svelte-store'
  import { match } from 'ts-pattern'

  const { pipelineName }: { pipelineName: string } = $props()
  const status = asyncDerived(readable(pipelineName), getPipelineStatus, {
    reloadable: true,
    initial: { status: 'Initializing' as const }
  })
  $effect(() => {
    let interval = setInterval(() => status.reload?.(), 2000)
    return () => {
      clearInterval(interval)
    }
  })
  const chipClass = derived(status, (status) => {
    return match(status.status)
      .with('Shutdown', () => 'preset-tonal-success')
      .with('Starting up', () => 'preset-tonal-surface')
      .with('Initializing', () => 'preset-tonal-warning')
      .with('Paused', () => 'preset-tonal-success')
      .with('Running', () => 'preset-tonal-success')
      .with('ShuttingDown', () => 'preset-tonal-surface')
      .with('Failed', () => 'preset-tonal-error')
      .with('Compiling sql', () => 'preset-tonal-warning')
      .with('Queued', () => 'preset-tonal-warning')
      .with('Compiling bin', () => 'preset-tonal-warning')
      .with('Program err', () => 'preset-tonal-error')
      .with('No program', () => 'preset-tonal-secondary')
      .exhaustive()
  })
</script>

<div class={'chip pointer-events-none h-6 w-24 uppercase ' + $chipClass}>{$status.status}</div>
<!-- {#each ['preset-tonal-primary', 'preset-tonal-secondary', 'preset-tonal-tertiary', 'preset-tonal-success', 'preset-tonal-warning', 'preset-tonal-error', 'preset-tonal-surface'] as color}
  <div class={'chip ' + color}>fffffffff</div>
{/each} -->
