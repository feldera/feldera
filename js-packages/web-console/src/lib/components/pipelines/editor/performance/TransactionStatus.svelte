<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { slide } from 'svelte/transition'
  import { formatQty } from '$lib/functions/format'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'

  let { metrics, class: _class = '' }: { metrics: { current: PipelineMetrics }; class?: string } =
    $props()

  const global = $derived(metrics.current.global)
  const transactionStatus = $derived(global.transaction_status)
  const commitProgress = $derived(global.commit_progress)
  const transactionId = $derived(global.transaction_id)

  const total = $derived(
    commitProgress
      ? commitProgress.completed + commitProgress.in_progress + commitProgress.remaining
      : 0
  )

  const completedPercent = $derived(
    total > 0 && commitProgress ? (commitProgress.completed / total) * 100 : 0
  )

  const inProgressFraction = $derived(
    commitProgress && commitProgress.in_progress_total_records > 0
      ? commitProgress.in_progress_processed_records / commitProgress.in_progress_total_records
      : 0
  )

  const combinedPercent = $derived(
    total > 0 && commitProgress
      ? ((commitProgress.completed + commitProgress.in_progress * inProgressFraction) / total) * 100
      : null
  )

  // Disable the smooth progress bar transition when the transaction changes so
  // the bar jumps immediately to the new value rather than animating.
  let disableTransition = $state(false)
  let prevTransactionId = transactionId

  $effect(() => {
    const currentId = transactionId
    if (currentId !== prevTransactionId) {
      prevTransactionId = currentId
      disableTransition = true
      requestAnimationFrame(() => {
        disableTransition = false
      })
    }
  })
</script>

{#if transactionStatus !== 'NoTransaction'}
  <div class="flex flex-wrap items-center gap-x-8 gap-y-2 {_class}" transition:slide>
    <div class="flex w-28 flex-col items-center mr-14">
      <div class="text-sm text-nowrap">Transaction status</div>
      <div class="pt-2 flex flex-nowrap justify-center items-center gap-2">
        <div></div>
        {#if transactionStatus === 'TransactionInProgress'}
          <div class="pointer-events-none chip bg-tertiary-50-950 uppercase">Started</div>
        {:else if transactionStatus === 'CommitInProgress'}
          <div class="pointer-events-none chip bg-warning-200-800 uppercase">Committing</div>
        {/if}
        <div class="font-dm-mono text-sm w-0 text-nowrap"><span class="select-none">ID:</span>{transactionId}</div>
      </div>
    </div>

    <div class="flex max-w-96 flex-1 flex-col">
      <div class="text-sm text-nowrap">
        {#if commitProgress}
          Operators:
          <span class="ml-2">Completed</span>
          <span class="font-dm-mono font-bold">{formatQty(commitProgress.completed)}</span> out of
          <span class="font-dm-mono font-bold">{formatQty(total)}</span>
          <span class="ml-2 select-none">·</span>
          <span class="ml-2">In progress</span>
          <span class="font-dm-mono font-bold">{formatQty(commitProgress.in_progress)}</span>
        {:else}
          &nbsp;
          <!-- <span class="ml-2 text-surface-500">—</span> -->
        {/if}
      </div>
      <div class="h-7 pt-3.5">
        <div class="relative">
          <Progress class="h-2" value={combinedPercent} max={100}>
            <Progress.Track class="bg-surface-600-400">
              <Progress.Range
                class="bg-yellow-500 {disableTransition ? 'duration-0' : 'duration-2000 ease-linear'}"
              />
            </Progress.Track>
          </Progress>
          <Progress class="absolute inset-x-0 bottom-0 h-2" value={completedPercent} max={100}>
            <Progress.Track class="opacity-0"></Progress.Track>
            <Progress.Range
              class="absolute inset-y-0 left-0 bg-success-500 {disableTransition
                ? 'duration-0'
                : 'duration-2000 ease-linear'}"
            />
          </Progress>
        </div>
      </div>
    </div>
  </div>
{/if}
