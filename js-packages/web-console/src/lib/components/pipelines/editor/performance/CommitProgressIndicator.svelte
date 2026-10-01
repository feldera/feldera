<script lang="ts">
  import { slide } from 'svelte/transition'
  import { match } from 'ts-pattern'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import { statusToneColors } from '$lib/functions/pipelineStatusColor'
  import CommitProgressRow from './CommitProgressRow.svelte'

  let { metrics, class: _class = '' }: { metrics: { current: PipelineMetrics }; class?: string } =
    $props()

  const global = $derived(metrics.current.global)
  const transactionId = $derived(global.transaction_id)
  const bootstrapPhase = $derived(global.concurrent_bootstrap_phase)

  const transactionStatus = $derived(
    match(global.transaction_status)
      .with('TransactionInProgress', () => ({
        label: 'Started',
        class: statusToneColors.blue.chip
      }))
      .with('CommitInProgress', () => ({
        label: 'Committing',
        class: statusToneColors.warning.chip
      }))
      // A pipeline that has not reported metrics yet has no status at all.
      .otherwise(() => null)
  )

  const bootstrapStatus = $derived(
    match(bootstrapPhase)
      // Both phases match the colour of the pipeline status chip.
      .with('ConcurrentBootstrapping', () => ({
        label: 'Backfilling',
        class: statusToneColors.blue.chip
      }))
      .with('Synchronizing', () => ({
        label: 'Synchronizing',
        class: statusToneColors.blue.chip
      }))
      .otherwise(() => null)
  )
</script>

<!-- The rows sit side by side while both fit at their natural width and wrap to
     their own lines when they do not. Each row carries its own basis and cap, so
     neither stretches to fill a line it has to itself. -->
<div class="flex w-full flex-wrap items-start gap-x-8 gap-y-4 {_class}" transition:slide>
  <!-- The transaction row is always present, reporting "None" when no transaction
       is running. -->
  <CommitProgressRow
    label="Transaction"
    status={transactionStatus}
    progress={global.commit_progress}
    idle="disable"
    resetKey={transactionId}
  >
    {#snippet detail()}
      <div class="font-dm-mono text-sm text-nowrap">
        <span class="select-none">ID:</span>{transactionId}
      </div>
    {/snippet}
  </CommitProgressRow>
  <CommitProgressRow
    label="Bootstrapping"
    status={bootstrapStatus}
    progress={global.concurrent_bootstrap_progress}
    idle="hide"
    resetKey={bootstrapPhase}
  />
</div>
