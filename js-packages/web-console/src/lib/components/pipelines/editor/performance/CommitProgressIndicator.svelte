<script lang="ts">
  import { untrack } from 'svelte'
  import { slide } from 'svelte/transition'
  import { match } from 'ts-pattern'
  import { LongCommitDetector } from '#lib/compositions/longCommit.ts'
  import { useInterpolatedValue } from '#lib/compositions/useInterpolatedValue.svelte.ts'
  import { PIPELINE_STATS_POLL_MS } from '#lib/compositions/useAggregatePipelineStats.svelte.ts'
  import { commitProgressPercents } from '#lib/functions/commitProgress.ts'
  import type { PipelineMetrics } from '#lib/functions/pipelineMetrics.ts'
  import { statusToneColors } from '#lib/functions/pipelineStatusColor.ts'
  import CommitProgressRow from './CommitProgressRow.svelte'

  let {
    metrics,
    pollPeriodMs = PIPELINE_STATS_POLL_MS,
    class: _class = ''
  }: {
    metrics: { current: PipelineMetrics }
    /** The time between two polls of `metrics`. */
    pollPeriodMs?: number
    class?: string
  } = $props()

  const global = $derived(metrics.current.global)
  const transactionId = $derived(global.transaction_id ?? 0)

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

  // The header counts up to each new transaction ID, at the rate that the ID grew.
  // When a transaction starts after a poll with no transaction, the ID jumps to its value.
  const shownTransactionId = useInterpolatedValue(
    () => ({ value: transactionId, key: transactionStatus !== null }),
    untrack(() => pollPeriodMs)
  )

  // The row shows the commit bar only for a long commit. A short commit ends
  // before its bar can show useful progress.
  const longCommit = new LongCommitDetector(untrack(() => pollPeriodMs))
  let isLongCommit = $state(false)
  // A pre-effect updates the flag before the row reads the same poll. Thus,
  // the row gets the poll and the flag together.
  $effect.pre(() => {
    isLongCommit = longCommit.update({
      transactionId,
      percent: commitProgressPercents(global.commit_progress).combined
    })
  })

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
    showBar={isLongCommit}
    {pollPeriodMs}
  >
    {#snippet header()}
      <!-- The line keeps its height while no transaction runs, so that the row
           does not change its height. -->
      <div class="flex h-[18px] items-center text-sm text-nowrap tabular-nums">
        {#if transactionStatus !== null}
          <span
            ><span class="select-none">ID:</span><span
              >{Math.round(shownTransactionId.current)}</span
            ></span
          >
        {/if}
      </div>
    {/snippet}
  </CommitProgressRow>
  <CommitProgressRow
    label="Bootstrapping"
    status={bootstrapStatus}
    progress={global.concurrent_bootstrap_progress}
    idle="hide"
    resetKey={bootstrapPhase}
    {pollPeriodMs}
  />
</div>
