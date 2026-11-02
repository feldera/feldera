<script lang="ts">
  import { statusChipClass, statusToneColors } from '#lib/functions/pipelineStatusColor.ts'
  import type { GlobalMetricsTimestamp } from '#lib/types/pipelineManager.ts'

  let {
    globalMetrics,
    onClick: onclick
  }: { globalMetrics: GlobalMetricsTimestamp; onClick?: () => void } = $props()

  // The tones match the transaction row of the performance tab.
  const chipClass = (tone: keyof typeof statusToneColors) =>
    `${statusChipClass} ${statusToneColors[tone].chip} h-6 cursor-pointer hover:brightness-95`
</script>

{#if globalMetrics.transaction_status === 'TransactionInProgress'}
  <button class={chipClass('blue')} {onclick}>
    <span class="hidden sm:inline">Transaction Started</span>
    <span class="sm:hidden">Started</span>
  </button>
{:else if globalMetrics.transaction_status === 'CommitInProgress'}
  <button class={chipClass('warning')} {onclick}>
    <span class="hidden sm:inline">Commit In Progress</span>
    <span class="sm:hidden">Committing</span>
  </button>
{/if}
