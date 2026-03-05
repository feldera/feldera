<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { format } from 'd3-format'
  import { slide } from 'svelte/transition'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'

  const formatQty = (v: number) => format(',.0f')(v)

  let { metrics }: { metrics: { current: PipelineMetrics } } = $props()

  const global = $derived(metrics.current.global)
  const transactionStatus = $derived(global.transaction_status)
  const commitProgress = $derived(global.commit_progress)
  const initiatedByApi = $derived(global.transaction_initiators?.initiated_by_api)

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
      : 0
  )
</script>

{#if transactionStatus !== 'NoTransaction'}
  <div class="mt-1 w-fit rounded">
    <!-- Composite progress slider showing 3 zones: completed (success) | in_progress fraction (warning) | remaining (track) -->
    <div class="relative -mt-1">
      <!-- Bottom Progress: shows completed + partial in_progress, warning color -->
      <Progress class="h-1" value={combinedPercent} max={100}>
        <Progress.Track class="bg-surface-600-400">
          <Progress.Range class="bg-yellow-500 duration-2000 ease-linear" />
        </Progress.Track>
      </Progress>
      <!-- Top Progress: shows completed only; track opacity-0 reveals bottom slider; range in success color -->
      <Progress class="absolute inset-0 h-1" value={completedPercent} max={100}>
        <Progress.Track class="opacity-0"></Progress.Track>
        <Progress.Range
          class="absolute inset-y-0 left-0 bg-success-500 duration-2000 ease-linear"
        />
      </Progress>
    </div>
    <div
      class="bg-white-dark scrollbar flex w-full overflow-x-auto"
      transition:slide={{ axis: 'y' }}
    >
      <table class="table h-min rounded text-base">
        <thead>
          <tr>
            <th>Transaction status</th>
            <th colspan={3}>Operators completed / in progress / total </th>
            <th>API-initiated phase</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>
              {#if transactionStatus === 'TransactionInProgress'}
                <div class="pointer-events-none chip bg-tertiary-50-950 uppercase">
                  Transaction Started
                </div>
              {:else if transactionStatus === 'CommitInProgress'}
                <div class="pointer-events-none chip bg-warning-200-800 uppercase">
                  Commit In Progress
                </div>
              {/if}
            </td>
            {#if commitProgress}
              <td class="text-end font-dm-mono">{formatQty(commitProgress.completed)}</td>
              <td class="text-end font-dm-mono">{formatQty(commitProgress.in_progress)}</td>
              <td class="text-end font-dm-mono">{formatQty(total)}</td>
            {:else}
              <td class="text-end font-dm-mono text-nowrap">-</td>
              <td class="text-end font-dm-mono text-nowrap">-</td>
              <td class="text-end font-dm-mono text-nowrap">-</td>
              <td class="text-end font-dm-mono text-nowrap">-</td>
            {/if}
            <td class="text-end">
              {#if initiatedByApi === 'Started'}
                <span class="fd fd-receipt-text mr-1 text-[16px] text-warning-500"></span>
                <Tooltip placement="top">Transaction started</Tooltip>
                Started
              {:else if initiatedByApi === 'Committed'}
                <span class="fd fd-receipt-text mr-1 text-[16px] text-success-500"></span>
                <Tooltip placement="top">Transaction committed</Tooltip>
                Committed
              {:else}
                None
              {/if}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
{/if}
