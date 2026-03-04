<script lang="ts">
  import type { GlobalMetricsTimestamp } from '$lib/types/pipelineManager'

  let {
    globalMetrics,
    onClick: onclick
  }: { globalMetrics: GlobalMetricsTimestamp; onClick?: () => void } = $props()
</script>

{#if globalMetrics.transaction_status === 'TransactionInProgress'}
  <button class="chip h-6 w-36 preset-filled-warning-200-800 px-2 uppercase" {onclick}>
    <span class="hidden sm:inline">TRANSACTION STARTED</span>
    <span class="sm:hidden">STARTED</span>
  </button>
{:else if globalMetrics.transaction_status === 'CommitInProgress'}
  <button class="chip h-6 w-36 preset-filled-warning-200-800 px-2 uppercase" {onclick}>
    <span class="hidden sm:inline">COMMIT IN PROGRESS</span>
    <span class="sm:hidden">COMMITTING</span>
  </button>
{/if}
