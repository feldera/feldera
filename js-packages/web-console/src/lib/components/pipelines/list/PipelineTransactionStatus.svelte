<script lang="ts">
  import { statusChipClass, statusToneColors } from '$lib/functions/pipelineStatusColor'
  import type { GlobalMetricsTimestamp } from '$lib/types/pipelineManager'

  let {
    globalMetrics,
    onClick: onclick
  }: { globalMetrics: GlobalMetricsTimestamp; onClick?: () => void } = $props()

  const chipClass = `${statusChipClass} ${statusToneColors.warning.chip} h-6 cursor-pointer hover:brightness-95`
</script>

{#if globalMetrics.transaction_status === 'TransactionInProgress'}
  <button class={chipClass} {onclick}>
    <span class="hidden sm:inline">Transaction Started</span>
    <span class="sm:hidden">Started</span>
  </button>
{:else if globalMetrics.transaction_status === 'CommitInProgress'}
  <button class={chipClass} {onclick}>
    <span class="hidden sm:inline">Commit In Progress</span>
    <span class="sm:hidden">Committing</span>
  </button>
{/if}
