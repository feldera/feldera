<script lang="ts">
  import type { ConcurrentBootstrapPhase } from '$lib/services/pipelineManager'

  const {
    phase,
    class: _class = '',
    'data-testid': testid
  }: {
    phase: ConcurrentBootstrapPhase
    class?: string
    'data-testid'?: string
  } = $props()

  // The two concurrent-bootstrap phases: backfilling in the background, then the
  // brief cutover window. Warning coloring on `Synchronizing` flags the pause.
  const style = $derived(
    phase === 'ConcurrentBootstrapping'
      ? { label: 'Bootstrapping Views', chip: 'bg-blue-200 dark:bg-blue-800' }
      : { label: 'Synchronizing', chip: 'preset-filled-warning-200-800' }
  )
</script>

{#if phase !== 'Inactive'}
  <div
    data-testid={testid}
    class={'chip px-2 uppercase transition-none ' + style.chip + ' ' + _class}
  >
    {style.label}
  </div>
{/if}
