<script lang="ts">
  import { format } from 'd3-format'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import type {
    AggregatedInputEndpointMetrics,
    AggregatedMetrics,
    AggregatedOutputEndpointMetrics,
    PipelineMetrics
  } from '$lib/functions/pipelineMetrics'
  import type { InputEndpointMetrics, OutputEndpointMetrics } from '$lib/services/manager'
  import type { Snippet } from '$lib/types/svelte'

  const formatQty = (v: number) => format(',.0f')(v)

  import type { ConnectorErrorFilter } from './ConnectorErrors.svelte'

  let {
    metrics,
    onConnectorSelect
  }: {
    metrics: { current: PipelineMetrics }
    onConnectorSelect: (
      relationName: string,
      connectorName: string,
      direction: 'input' | 'output',
      filter: ConnectorErrorFilter
    ) => void
  } = $props()

  // List of tables and views that have been expanded to view individual connectors
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

  const inputHasErrors = (connector: AggregatedInputEndpointMetrics['connectors'][0]) =>
    (connector.metrics.num_transport_errors ?? 0) > 0 ||
    (connector.metrics.num_parse_errors ?? 0) > 0

  const outputHasErrors = (connector: AggregatedOutputEndpointMetrics['connectors'][0]) =>
    (connector.metrics.num_transport_errors ?? 0) > 0 ||
    (connector.metrics.num_encode_errors ?? 0) > 0
</script>

{#snippet chevron(isExpanded: boolean)}
  <div
    class="fd fd-chevron-down mr-1 inline-block w-4 text-center text-[16px]"
    class:rotate-180={isExpanded}
  ></div>
{/snippet}

{#snippet chevronSpacer()}
  <div class="mr-1 inline-block w-4"></div>
{/snippet}

{#snippet inputConnectorIcons(
  paused: boolean | undefined,
  hasErrors: boolean,
  barrier: boolean | undefined,
  transactionPhase: 'started' | 'committed' | undefined,
  endOfInput: boolean,
  onErrorClick?: (e: Event) => void
)}
  {#if barrier}
    <span class="fd fd-construction mr-1 text-[16px] text-warning-500"></span>
    <Tooltip placement="top">Commit blocked by this input</Tooltip>
  {:else if hasErrors}
    <span
      class="fd fd-circle-alert mr-1 cursor-pointer text-[16px] text-error-500"
      onclick={(e) => {
        e.stopPropagation()
        onErrorClick?.(e)
      }}
      role="button"
      tabindex="0"
      onkeydown={(e) => e.key === 'Enter' && onErrorClick?.(e)}
    ></span>
    <Tooltip placement="top">Parse or transport errors occurred — click to view</Tooltip>
  {/if}
  {#if transactionPhase === 'started'}
    <span class="fd fd-receipt-text mr-1 text-[16px] text-warning-500"></span>
    <Tooltip placement="top">Transaction started</Tooltip>
  {:else if transactionPhase === 'committed'}
    <span class="fd fd-receipt-text mr-1 text-[16px] text-success-500"></span>
    <Tooltip placement="top">Transaction committed</Tooltip>
  {:else if endOfInput}
    <span class="fd fd-circle-dot mr-1 text-[16px] text-surface-700-300"></span>
    <Tooltip placement="top">End of input</Tooltip>
  {:else if paused}
    <span class="fd fd-circle-pause mr-1 text-[16px] text-surface-700-300"></span>
    <Tooltip placement="top">Paused</Tooltip>
  {:else}
    <span class="fd fd-circle-play mr-1 text-[16px] text-success-500"></span>
    <Tooltip placement="top">Running</Tooltip>
  {/if}
{/snippet}

{#snippet outputConnectorIcons(hasErrors: boolean, onErrorClick?: (e: Event) => void)}
  {#if hasErrors}
    <span
      class="fd fd-circle-alert mr-1 cursor-pointer text-[16px] text-error-500"
      onclick={(e) => {
        e.stopPropagation()
        onErrorClick?.(e)
      }}
      role="button"
      tabindex="0"
      onkeydown={(e) => e.key === 'Enter' && onErrorClick?.(e)}
    ></span>
    <Tooltip placement="top">Encode or transport errors occurred — click to view</Tooltip>
  {/if}
{/snippet}

{#snippet connectorName(name: string)}
  <span class="relative h-5 min-w-0 flex-1 overflow-hidden">
    <span class="absolute inset-0 overflow-hidden text-left text-nowrap text-ellipsis" dir="rtl">
      {name}
    </span>
  </span>
{/snippet}

{#snippet inputConnectorName(
  connector: AggregatedInputEndpointMetrics['connectors'][0],
  relation: string
)}
  <div class="flex min-w-0 flex-nowrap">
    <span class="flex w-10 shrink-0 flex-nowrap justify-end">
      {@render inputConnectorIcons(
        connector.paused,
        inputHasErrors(connector),
        connector.barrier,
        connector.transaction_phase,
        connector.metrics.end_of_input,
        () => onConnectorSelect(relation, connector.endpointName, 'input', 'all')
      )}
    </span>
    {@render connectorName(connector.endpointName)}
  </div>
{/snippet}

{#snippet outputConnectorName(
  connector: AggregatedOutputEndpointMetrics['connectors'][0],
  relation: string
)}
  <div class="flex min-w-0 flex-nowrap">
    <span class="flex w-10 shrink-0 flex-nowrap justify-end">
      {@render outputConnectorIcons(outputHasErrors(connector), () =>
        onConnectorSelect(relation, connector.endpointName, 'output', 'all')
      )}
    </span>
    {@render connectorName(connector.endpointName)}
  </div>
{/snippet}

{#snippet tableColumnHeaders()}
  <tr>
    <th class="font-normal" rowspan="2">Table</th>
    <th class="w-full pb-0! font-normal" rowspan="2"><span class="pl-10">Connector</span></th>
    <th class="pb-0! text-center! font-normal" colspan="2">Ingested</th>
    <th class="pb-0! text-center! font-normal" colspan="2">Buffered</th>
    <th class="font-normal 2xl:text-nowrap" rowspan="2">Parse errors</th>
    <th class="font-normal 2xl:text-nowrap" rowspan="2">Transport errors</th>
  </tr>
  <tr>
    <th class="pt-0! !text-end font-normal">records</th>
    <th class="pt-0! !text-end font-normal">bytes</th>
    <th class="pt-0! !text-end font-normal">records</th>
    <th class="pt-0! !text-end font-normal">bytes</th>
  </tr>
{/snippet}

{#snippet inputMetricsCells(
  m: InputEndpointMetrics,
  ioActive?: boolean,
  relation?: string,
  connectorEndpointName?: string
)}
  <td class="text-end font-dm-mono text-nowrap"
    ><span class={ioActive ? 'text-success-600-400' : ''}>{formatQty(m.total_records)}</span></td
  >
  <td class="text-end font-dm-mono text-nowrap"
    ><span class={ioActive ? 'text-success-600-400' : ''}>{humanSize(m.total_bytes)}</span></td
  >
  <td class="text-end font-dm-mono text-nowrap">{formatQty(m.buffered_records)}</td>
  <td class="text-end font-dm-mono text-nowrap">{humanSize(m.buffered_bytes)}</td>
  <td class="text-end font-dm-mono text-nowrap">
    {#if m.num_parse_errors > 0 && relation && connectorEndpointName}
      <button
        class="cursor-pointer font-dm-mono text-error-500 hover:underline p-2 -m-2"
        onclick={(e) => {
          e.stopPropagation()
          onConnectorSelect(relation, connectorEndpointName, 'input', 'parse')
        }}>{formatQty(m.num_parse_errors)}</button
      >
    {:else}
      {formatQty(m.num_parse_errors)}
    {/if}
  </td>
  <td class="text-end font-dm-mono text-nowrap">
    {#if m.num_transport_errors > 0 && relation && connectorEndpointName}
      <button
        class="cursor-pointer font-dm-mono text-error-500 hover:underline p-2 -m-2"
        onclick={(e) => {
          e.stopPropagation()
          onConnectorSelect(relation, connectorEndpointName, 'input', 'transport')
        }}>{formatQty(m.num_transport_errors)}</button
      >
    {:else}
      {formatQty(m.num_transport_errors)}
    {/if}
  </td>
{/snippet}

{#snippet viewColumnHeaders()}
  <tr>
    <th class="font-normal" rowspan="2">View</th>
    <th class="w-full pb-0! font-normal" rowspan="2"><span class="pl-10">Connector</span></th>
    <th class="pb-0! text-center! font-normal" colspan="2">Transmitted</th>
    <th class="pb-0! text-center! font-normal" colspan="2">Buffered</th>
    <th class="pb-0! text-center! font-normal" colspan="2">Queued</th>
    <th class="font-normal 2xl:text-nowrap" rowspan="2">Encode errors</th>
    <th class="font-normal 2xl:text-nowrap" rowspan="2">Transport errors</th>
  </tr>
  <tr>
    <th class="pt-0! !text-end font-normal">records</th>
    <th class="pt-0! !text-end font-normal">bytes</th>
    <th class="pt-0! !text-end font-normal">records</th>
    <th class="pt-0! !text-end font-normal">batches</th>
    <th class="pt-0! !text-end font-normal">records</th>
    <th class="pt-0! !text-end font-normal">batches</th>
  </tr>
{/snippet}

{#snippet outputMetricsCells(
  m: OutputEndpointMetrics,
  ioActive?: boolean,
  relation?: string,
  connectorEndpointName?: string
)}
  <td class="text-end font-dm-mono text-nowrap"
    ><span class={ioActive ? 'text-success-600-400' : ''}>{formatQty(m.transmitted_records)}</span
    ></td
  >
  <td class="text-end font-dm-mono text-nowrap"
    ><span class={ioActive ? 'text-success-600-400' : ''}>{humanSize(m.transmitted_bytes)}</span
    ></td
  >
  <td class="text-end font-dm-mono text-nowrap">{formatQty(m.buffered_records)}</td>
  <td class="text-end font-dm-mono text-nowrap">{formatQty(m.buffered_batches)}</td>
  <td class="text-end font-dm-mono text-nowrap">{formatQty(m.queued_records)}</td>
  <td class="text-end font-dm-mono text-nowrap">{formatQty(m.queued_batches)}</td>
  <td class="text-end font-dm-mono text-nowrap">
    {#if m.num_encode_errors > 0 && relation && connectorEndpointName}
      <button
        class="cursor-pointer font-dm-mono text-error-500 hover:underline p-2 -m-2"
        onclick={(e) => {
          e.stopPropagation()
          onConnectorSelect(relation, connectorEndpointName, 'output', 'encode')
        }}>{formatQty(m.num_encode_errors)}</button
      >
    {:else}
      {formatQty(m.num_encode_errors)}
    {/if}
  </td>
  <td class="text-end font-dm-mono text-nowrap">
    {#if m.num_transport_errors > 0 && relation && connectorEndpointName}
      <button
        class="cursor-pointer font-dm-mono text-error-500 hover:underline p-2 -m-2"
        onclick={(e) => {
          e.stopPropagation()
          onConnectorSelect(relation, connectorEndpointName, 'output', 'transport')
        }}>{formatQty(m.num_transport_errors)}</button
      >
    {:else}
      {formatQty(m.num_transport_errors)}
    {/if}
  </td>
{/snippet}

{#snippet tableMultiConnectorCell(data: AggregatedInputEndpointMetrics, isExpanded: boolean)}
  {@const runningCount = data.connectors.filter((c) => c.paused === false).length}
  {@const anyErrors = data.connectors.some(inputHasErrors)}
  {@const anyBarrier = data.connectors.some((c) => c.barrier === true)}
  {@const aggregateTransactionPhase = data.connectors.some((c) => c.transaction_phase === 'started')
    ? 'started'
    : data.connectors.some((c) => c.transaction_phase === 'committed')
      ? 'committed'
      : undefined}
  <div class="flex flex-nowrap">
    <span class="flex w-10 flex-nowrap justify-end">
      {#if !isExpanded}
        {@render inputConnectorIcons(
          runningCount > 0 ? false : true,
          anyErrors,
          anyBarrier,
          aggregateTransactionPhase,
          data.aggregate.metrics.end_of_input
        )}
      {:else}
        <span class="pl-6"></span>
      {/if}
    </span>
    {runningCount} / {data.connectors.length} running
  </div>
{/snippet}

{#snippet viewMultiConnectorCell(data: AggregatedOutputEndpointMetrics, isExpanded: boolean)}
  {#if !isExpanded && data.connectors.some(outputHasErrors)}
    <span class="fd fd-circle-alert mr-1 text-[16px] text-error-500"></span>
  {/if}
  {data.connectors.length} connectors
{/snippet}

{#snippet metricsTable<EndpointMetrics, Extra extends { io_active: boolean }>(
  maxWidth: string,
  tableData: Map<string, AggregatedMetrics<EndpointMetrics, Extra>>,
  expanded: Set<string>,
  toggle: (r: string) => void,
  connectorNameSnippet: Snippet<
    [AggregatedMetrics<EndpointMetrics, Extra>['connectors'][0], string]
  >,
  multiConnectorRelationCell: Snippet<[AggregatedMetrics<EndpointMetrics, Extra>, boolean]>,
  columnHeaders: Snippet,
  metricsCells: Snippet<[EndpointMetrics, boolean | undefined, string?, string?]>
)}
  <div class="scrollbar w-full overflow-x-auto {maxWidth}">
    <table class="bg-white-dark table h-min rounded text-base">
      <thead>
        {@render columnHeaders()}
      </thead>
      <tbody>
        {#each tableData.entries() as [relation, data]}
          {@const isExpanded = expanded.has(relation)}
          {@const hasMultipleConnectors = data.connectors.length > 1}
          <tr
            class={hasMultipleConnectors ? 'cursor-pointer hover:bg-surface-50-950' : ''}
            onclick={() => hasMultipleConnectors && toggle(relation)}
          >
            <td class="font-medium">
              <div class="text-nowrap">
                {#if hasMultipleConnectors}
                  {@render chevron(isExpanded)}
                {:else}
                  {@render chevronSpacer()}
                {/if}
                {relation}
              </div>
            </td>
            <td>
              {#if hasMultipleConnectors}
                {@render multiConnectorRelationCell(data, isExpanded)}
              {:else if data.connectors.length === 1}
                {@render connectorNameSnippet(data.connectors[0], relation)}
              {/if}
            </td>
            {@render metricsCells(
              data.aggregate.metrics,
              data.connectors.some((c) => c.io_active),
              hasMultipleConnectors ? undefined : relation,
              hasMultipleConnectors ? undefined : data.connectors[0]?.endpointName
            )}
          </tr>
          {#if isExpanded && hasMultipleConnectors}
            {#each data.connectors as connector}
              <tr>
                <td></td>
                <td>{@render connectorNameSnippet(connector, relation)}</td>
                {@render metricsCells(
                  connector.metrics,
                  connector.io_active,
                  relation,
                  connector.endpointName
                )}
              </tr>
            {/each}
          {/if}
        {/each}
      </tbody>
    </table>
  </div>
{/snippet}

{#if metrics.current.tables.size}
  {@render metricsTable(
    'max-w-[1540px]',
    metrics.current.tables,
    expandedTables,
    toggleTable,
    inputConnectorName,
    tableMultiConnectorCell,
    tableColumnHeaders,
    inputMetricsCells
  )}
{/if}
{#if metrics.current.views.size}
  {@render metricsTable(
    'max-w-[1540px]',
    metrics.current.views,
    expandedViews,
    toggleView,
    outputConnectorName,
    viewMultiConnectorCell,
    viewColumnHeaders,
    outputMetricsCells
  )}
{/if}
