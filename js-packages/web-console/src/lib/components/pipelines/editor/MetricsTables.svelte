<script lang="ts">
  import { format } from 'd3-format'
  import { humanSize } from '$lib/functions/common/string'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'

  const formatQty = (v: number) => format(',.0f')(v)

  let { metrics }: { metrics: { current: PipelineMetrics } } = $props()

  let expandedTables = $state<Set<string>>(new Set())
  let expandedViews = $state<Set<string>>(new Set())

  const toggleTable = (relation: string) => {
    if (expandedTables.has(relation)) {
      expandedTables.delete(relation)
    } else {
      expandedTables.add(relation)
    }
    expandedTables = new Set(expandedTables)
  }

  const toggleView = (relation: string) => {
    if (expandedViews.has(relation)) {
      expandedViews.delete(relation)
    } else {
      expandedViews.add(relation)
    }
    expandedViews = new Set(expandedViews)
  }
</script>

{#if metrics.current.tables.size}
  <table class="bg-white-dark table h-min max-w-[1200px] rounded text-base">
    <thead>
      <tr>
        <th class="font-normal">Table</th>
        <th class="font-normal">Connector</th>
        <th class="!text-end font-normal">Ingested records</th>
        <th class="!text-end font-normal">Ingested bytes</th>
        <th class="!text-end font-normal">Parse errors</th>
        <th class="!text-end font-normal">Transport errors</th>
      </tr>
    </thead>
    <tbody>
      {#each metrics.current.tables.entries() as [relation, data]}
        {@const isExpanded = expandedTables.has(relation)}
        {@const hasMultipleConnectors = data.connectors.length > 1}
        <tr
          class={hasMultipleConnectors ? 'cursor-pointer hover:bg-surface-50-950' : ''}
          onclick={() => hasMultipleConnectors && toggleTable(relation)}
        >
          <td class="font-medium">
            {relation}
          </td>
          <td class="">
            {#if hasMultipleConnectors}
              <div
                class="fd fd-chevron-down mr-1 inline-block w-4 text-center text-[16px]"
                class:rotate-180={isExpanded}
              ></div>
              ({data.connectors.length} connectors)
            {:else if data.connectors.length === 1}
              {data.connectors[0].endpointName}
            {:else}
              —
            {/if}
          </td>
          <td class="text-end font-dm-mono">
            {formatQty(data.aggregate.total_records)}
          </td>
          <td class="text-end font-dm-mono">
            {humanSize(data.aggregate.total_bytes)}
          </td>
          <td class="text-end font-dm-mono">{formatQty(data.aggregate.num_parse_errors)}</td>
          <td class="text-end font-dm-mono">{formatQty(data.aggregate.num_transport_errors)}</td>
        </tr>
        {#if isExpanded && hasMultipleConnectors}
          {#each data.connectors as connector}
            <tr class="">
              <td></td>
              <td class="">
                <div class="pl-6">
                  {connector.endpointName}
                </div>
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.total_records)}
              </td>
              <td class="text-end font-dm-mono">
                {humanSize(connector.metrics.total_bytes)}
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.num_parse_errors)}
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.num_transport_errors)}
              </td>
            </tr>
          {/each}
        {/if}
      {/each}
    </tbody>
  </table>
{/if}
{#if metrics.current.views.size}
  <table class="bg-white-dark table h-min max-w-[1700px] rounded text-base">
    <thead>
      <tr>
        <th class="font-normal">View</th>
        <th class="font-normal">Connector</th>
        <th class="!text-end font-normal">Transmitted records</th>
        <th class="!text-end font-normal">Transmitted bytes</th>
        <th class="!text-end font-normal">Buffered records</th>
        <th class="!text-end font-normal">Queued records</th>
        <th class="!text-end font-normal">Buffered batches</th>
        <th class="!text-end font-normal">Queued batches</th>
        <th class="!text-end font-normal">Encode errors</th>
        <th class="!text-end font-normal">Transport errors</th>
      </tr>
    </thead>
    <tbody>
      {#each metrics.current.views.entries() as [relation, data]}
        {@const isExpanded = expandedViews.has(relation)}
        {@const hasMultipleConnectors = data.connectors.length > 1}
        <tr
          class={hasMultipleConnectors ? 'cursor-pointer hover:bg-surface-50-950' : ''}
          onclick={() => hasMultipleConnectors && toggleView(relation)}
        >
          <td class="font-medium">
            {relation}
          </td>
          <td class="">
            {#if hasMultipleConnectors}
              <div
                class="fd fd-chevron-down mr-1 inline-block w-4 text-center text-[16px]"
                class:rotate-180={isExpanded}
              ></div>
              ({data.connectors.length} connectors)
            {:else if data.connectors.length === 1}
              {data.connectors[0].endpointName}
            {:else}
              —
            {/if}
          </td>
          <td class="text-end font-dm-mono">
            {formatQty(data.aggregate.transmitted_records)}
          </td>
          <td class="text-end font-dm-mono">
            {humanSize(data.aggregate.transmitted_bytes)}
          </td>
          <td class="text-end font-dm-mono">{formatQty(data.aggregate.buffered_records)}</td>
          <td class="text-end font-dm-mono">{formatQty(data.aggregate.queued_records)}</td>
          <td class="text-end font-dm-mono">{formatQty(data.aggregate.buffered_batches)}</td>
          <td class="text-end font-dm-mono">{formatQty(data.aggregate.queued_batches)}</td>
          <td class="text-end font-dm-mono">{formatQty(data.aggregate.num_encode_errors)}</td>
          <td class="text-end font-dm-mono">{formatQty(data.aggregate.num_transport_errors)}</td>
        </tr>
        {#if isExpanded && hasMultipleConnectors}
          {#each data.connectors as connector}
            <tr class="">
              <td></td>
              <td class="">
                <div class="pl-6">
                  {connector.endpointName}
                </div>
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.transmitted_records)}
              </td>
              <td class="text-end font-dm-mono">
                {humanSize(connector.metrics.transmitted_bytes)}
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.buffered_records)}
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.queued_records)}
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.buffered_batches)}
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.queued_batches)}
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.num_encode_errors)}
              </td>
              <td class="text-end font-dm-mono">
                {formatQty(connector.metrics.num_transport_errors)}
              </td>
            </tr>
          {/each}
        {/if}
      {/each}
    </tbody>
  </table>
{/if}
