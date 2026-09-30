<script lang="ts">
  import { ConnectorMetricsTables } from 'common-ui'
  import type { AnalysisTabProps } from './MetricsTab.svelte'

  let { connectorMetrics }: AnalysisTabProps = $props()
</script>

<div class="scrollbar flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto">
  {#if connectorMetrics && (connectorMetrics.tables.size || connectorMetrics.views.size)}
    <ConnectorMetricsTables metrics={{ current: connectorMetrics }} />
  {:else}
    <div class="flex h-full items-center justify-center text-sm text-surface-600-400">
      No connector statistics available in this bundle
    </div>
  {/if}
</div>
