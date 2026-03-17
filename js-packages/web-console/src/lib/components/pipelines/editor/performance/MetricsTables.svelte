<script lang="ts">
  import { SegmentedControl } from '@skeletonlabs/skeleton-svelte'
  import { format } from 'd3-format'
  import type { HTMLAttributes } from 'svelte/elements'
  import Popover from '$lib/components/common/Popover.svelte'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import { count } from '$lib/functions/common/array'
  import { humanSize } from '$lib/functions/common/string'
  import type {
    AggregatedInputEndpointMetrics,
    AggregatedMetrics,
    AggregatedOutputEndpointMetrics,
    PipelineMetrics
  } from '$lib/functions/pipelineMetrics'
  import type { InputEndpointMetrics, OutputEndpointMetrics } from '$lib/services/manager'
  import type { Snippet } from '$lib/types/svelte'
  import type { ConnectorErrorFilter } from './ConnectorErrors.svelte'

  const formatQty = (v: number) => format(',.0f')(v)

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

  type HealthFilter = 'all' | 'unhealthy'
  let tableHealthFilter = $state<HealthFilter>('all')
  let viewHealthFilter = $state<HealthFilter>('all')
  const healthFilterModes: HealthFilter[] = ['all', 'unhealthy']

  const isUnhealthy = (connector: { health?: { status: string } | null }) =>
    connector.health?.status === 'Unhealthy'

  const unhealthyInputCount = $derived(
    count(metrics.current.tables, (data) => count(data.connectors, isUnhealthy))
  )
  const unhealthyOutputCount = $derived(
    count(metrics.current.views, (data) => count(data.connectors, isUnhealthy))
  )

  const filteredTables = $derived.by(() => {
    if (tableHealthFilter === 'all') {
      return metrics.current.tables
    }
    const filtered = new Map<string, AggregatedInputEndpointMetrics>()
    for (const [relation, data] of metrics.current.tables) {
      const unhealthyConnectors = data.connectors.filter(isUnhealthy)
      if (unhealthyConnectors.length > 0) {
        filtered.set(relation, { ...data, connectors: unhealthyConnectors })
      }
    }
    return filtered
  })

  const filteredViews = $derived.by(() => {
    if (viewHealthFilter === 'all') {
      return metrics.current.views
    }
    const filtered = new Map<string, AggregatedOutputEndpointMetrics>()
    for (const [relation, data] of metrics.current.views) {
      const unhealthyConnectors = data.connectors.filter(isUnhealthy)
      if (unhealthyConnectors.length > 0) {
        filtered.set(relation, { ...data, connectors: unhealthyConnectors })
      }
    }
    return filtered
  })

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

{#snippet inputConnectorIcons({
  paused,
  hasErrors,
  hasFatalError,
  barrier,
  endOfInput,
  onErrorClick
}: {
  paused: boolean | undefined
  hasErrors: boolean
  hasFatalError: boolean
  barrier: boolean | undefined
  endOfInput: boolean
  onErrorClick?: (e: Event) => void
})}
  {@const gotoErrorsBtnProps: HTMLAttributes<HTMLSpanElement> = onErrorClick
        ? {
            onclick: (e) => {
              e.stopPropagation()
              onErrorClick?.(e)
            },
            role: 'button',
            tabindex: 0,
            onkeydown: (e) => e.key === 'Enter' && onErrorClick?.(e)
          }
        : {}}
  {#if barrier}
    <span data-testid="box-icon-barrier" class="fd fd-construction text-[16px] text-warning-500"
    ></span>
    <Tooltip placement="top">Checkpointing is blocked by this connector</Tooltip>
  {:else if hasErrors}
    <span
      data-testid="btn-icon-input-errors"
      class="fd fd-circle-alert text-[16px] text-error-500"
      {...gotoErrorsBtnProps}
    ></span>
    <Tooltip placement="top"
      >Parse or transport errors occurred{onErrorClick ? ' — click to view' : ''}</Tooltip
    >
  {/if}
  {#if endOfInput}
    <span
      data-testid="box-icon-end-of-input"
      class="fd fd-circle-dot text-[16px] text-surface-700-300"
    ></span>
    <Tooltip placement="top">End of input</Tooltip>
  {:else if paused}
    <span data-testid="box-icon-paused" class="fd fd-circle-pause text-[16px] text-surface-700-300"
    ></span>
    <Tooltip placement="top">Paused</Tooltip>
  {:else if hasFatalError}
    <span
      data-testid="btn-icon-input-fatal-error"
      class="fd fd-circle-x text-[16px] text-error-500"
      {...gotoErrorsBtnProps}
    ></span>
    <Tooltip placement="top"
      >{onErrorClick
        ? `Fatal error occurred — connector can't ingest more data — click to view`
        : `Fatal error occurred in one of the connectors`}</Tooltip
    >
  {:else}
    <span data-testid="box-icon-running" class="fd fd-circle-play text-[16px] text-success-500"
    ></span>
    <Tooltip placement="top">Running</Tooltip>
  {/if}
{/snippet}

{#snippet inputTransactionCell(transactionPhase: 'started' | 'committed' | undefined)}
  {#if transactionPhase === 'started'}
    <span
      data-testid="box-icon-transaction-started"
      class="-my-1 chip preset-filled-warning-50-950 uppercase text-xs"
    >Started</span>
  {:else if transactionPhase === 'committed'}
    <span
      data-testid="box-icon-transaction-committed"
      class="-my-1 chip preset-filled-success-50-950 uppercase text-xs"
    >Ready to commit</span>
  {/if}
{/snippet}

{#snippet outputConnectorIcons({
  hasErrors,
  hasFatalError,
  onErrorClick
}: {
  hasErrors: boolean
  hasFatalError: boolean
  onErrorClick?: (e: Event) => void
})}
  {#if hasErrors}
    <span
      data-testid="btn-icon-output-errors"
      class="fd {hasFatalError ? 'fd-circle-x' : 'fd-circle-alert'} text-[16px] text-error-500"
      {...onErrorClick
        ? {
            onclick: (e) => {
              e.stopPropagation()
              onErrorClick?.(e)
            },
            role: 'button',
            tabindex: 0,
            onkeydown: (e) => e.key === 'Enter' && onErrorClick?.(e)
          }
        : {}}
    ></span>
    <Tooltip placement="top"
      >{hasFatalError
        ? onErrorClick
          ? `Fatal error occurred — connector can't output more data — click to view`
          : `Fatal error occurred in one of the connectors`
        : `Encode or transport errors occurred${onErrorClick ? ' — click to view' : ''}`}</Tooltip
    >
  {/if}
{/snippet}

{#snippet connectorName(name: string, end?: Snippet)}
  <span class="relative -mb-1 h-6 min-w-0 flex-1 overflow-hidden">
    <span class="absolute inset-0 overflow-hidden text-left text-nowrap text-ellipsis" dir="rtl">
      {@render end?.()}
      {name}
    </span>
  </span>
{/snippet}

{#snippet unhealthyChip(description: string)}
  <span
    data-testid="box-unhealthy-chip"
    class="-my-1 ml-2 chip preset-filled-error-50-950 uppercase">unhealthy</span
  >
  <Popover class="z-20 max-w-lg">
    <div class="flex flex-row-reverse flex-nowrap items-start gap-4">
      {#if description}
        <span class="min-w-0 flex-1 wrap-break-word whitespace-pre-wrap">
          {description}
        </span>
        <ClipboardCopyButton class="-m-2" value={description}></ClipboardCopyButton>
      {:else}
        <i>Connector is unhealthy. Details are not available.</i>
      {/if}
    </div>
  </Popover>
{/snippet}

{#snippet inputConnectorName(
  connector: AggregatedInputEndpointMetrics['connectors'][0],
  relation: string,
  showHealthChip?: boolean
)}
  <td>
    <div class="flex min-w-0 flex-nowrap items-center">
      <span class="mr-1 flex w-10 flex-nowrap justify-end gap-1">
        {@render inputConnectorIcons({
          paused: connector.paused,
          hasErrors: inputHasErrors(connector),
          hasFatalError: connector.fatal_error != null,
          barrier: connector.barrier,
          endOfInput: connector.metrics.end_of_input,
          onErrorClick: () => onConnectorSelect(relation, connector.endpointName, 'input', 'all')
        })}
      </span>
      {#snippet end()}
        {#if showHealthChip && connector.health?.status === 'Unhealthy'}
          {@render unhealthyChip(connector.health.description ?? '')}
        {/if}
      {/snippet}
      {@render connectorName(connector.endpointName, end)}
    </div>
  </td>
  <td class="text-center">{@render inputTransactionCell(connector.transaction_phase)}</td>
{/snippet}

{#snippet outputConnectorName(
  connector: AggregatedOutputEndpointMetrics['connectors'][0],
  relation: string,
  showHealthChip?: boolean
)}
  <td>
    <div class="flex min-w-0 flex-nowrap items-center">
      <span class="flex w-10 shrink-0 flex-nowrap justify-end gap-1">
        {@render outputConnectorIcons({
          hasErrors: outputHasErrors(connector),
          hasFatalError: connector.fatal_error != null,
          onErrorClick: () => onConnectorSelect(relation, connector.endpointName, 'output', 'all')
        })}
      </span>
      {#snippet end()}
        {#if showHealthChip && connector.health?.status === 'Unhealthy'}
          {@render unhealthyChip(connector.health.description ?? '')}
        {/if}
      {/snippet}
      {@render connectorName(connector.endpointName, end)}
    </div>
  </td>
{/snippet}

{#snippet connectorHealthFilterControl(
  unhealthyCount: number,
  filter: HealthFilter,
  setFilter: (v: HealthFilter) => void
)}
  <SegmentedControl
    value={filter}
    onValueChange={(e) => setFilter(e.value as HealthFilter)}
    class="-mt-2"
  >
    <SegmentedControl.Label />
    <SegmentedControl.Control class="w-fit flex-none rounded preset-filled-surface-50-950 p-1">
      <SegmentedControl.Indicator class="bg-white-dark shadow" />
      {#each healthFilterModes as mode}
        <SegmentedControl.Item
          value={mode}
          class="btn h-6 cursor-pointer px-3"
          data-testid="btn-select-{mode}"
        >
          <SegmentedControl.ItemText class="text-surface-950-50 capitalize">
            {mode}{#if mode === 'unhealthy' && unhealthyCount > 0}<span
                class="ml-2 rounded bg-error-50-950 px-2">{unhealthyCount}</span
              >{/if}
          </SegmentedControl.ItemText>
          <SegmentedControl.ItemHiddenInput />
        </SegmentedControl.Item>
      {/each}
    </SegmentedControl.Control>
  </SegmentedControl>
{/snippet}

{#snippet tableColumnHeaders()}
  <tr>
    <th class="font-normal" rowspan="2">Table</th>
    <th class="w-full font-normal" rowspan="2">
      <div class="flex items-center gap-4 pl-10">
        <span>Connectors</span>
        {@render connectorHealthFilterControl(
          unhealthyInputCount,
          tableHealthFilter,
          (v) => (tableHealthFilter = v)
        )}
      </div>
    </th>
    <th class="font-normal" rowspan="2">Transaction</th>
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
  <td class="text-end font-dm-mono text-nowrap {m.num_parse_errors > 0 ? 'text-error-500' : ''}">
    {#if m.num_parse_errors > 0 && relation && connectorEndpointName}
      <button
        data-testid="btn-parse-errors"
        class="-m-2 cursor-pointer p-2 font-dm-mono text-error-500 hover:underline"
        onclick={(e) => {
          e.stopPropagation()
          onConnectorSelect(relation, connectorEndpointName, 'input', 'parse')
        }}>{formatQty(m.num_parse_errors)}</button
      >
    {:else}
      {formatQty(m.num_parse_errors)}
    {/if}
  </td>
  <td
    class="text-end font-dm-mono text-nowrap {m.num_transport_errors > 0 ? 'text-error-500' : ''}"
  >
    {#if m.num_transport_errors > 0 && relation && connectorEndpointName}
      <button
        data-testid="btn-input-transport-errors"
        class="-m-2 cursor-pointer p-2 font-dm-mono text-error-500 hover:underline"
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
    <th class="w-full font-normal" rowspan="2">
      <div class="flex items-center gap-4 pl-10">
        <span>Connectors</span>
        {@render connectorHealthFilterControl(
          unhealthyOutputCount,
          viewHealthFilter,
          (v) => (viewHealthFilter = v)
        )}
      </div>
    </th>
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
        data-testid="btn-encode-errors"
        class="-m-2 cursor-pointer p-2 font-dm-mono text-error-500 hover:underline"
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
        data-testid="btn-output-transport-errors"
        class="-m-2 cursor-pointer p-2 font-dm-mono text-error-500 hover:underline"
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
  {@const anyFatalError = data.connectors.some((c) => c.fatal_error != null)}
  {@const anyBarrier = data.connectors.some((c) => c.barrier === true)}
  {@const anyUnhealthy = data.connectors.some(isUnhealthy)}
  {@const aggregateTransactionPhase = data.connectors.some((c) => c.transaction_phase === 'started')
    ? 'started'
    : data.connectors.some((c) => c.transaction_phase === 'committed')
      ? 'committed'
      : undefined}
  <td>
    <div data-testid="box-multi-connector-summary" class="flex flex-nowrap items-center">
      <span class="mr-1 flex w-10 flex-nowrap justify-end gap-1">
        {#if !isExpanded}
          {@render inputConnectorIcons({
            paused: runningCount > 0 ? false : true,
            hasErrors: anyErrors,
            hasFatalError: anyFatalError,
            barrier: anyBarrier,
            endOfInput: data.aggregate.metrics.end_of_input
          })}
        {:else}
          <span class="pl-6"></span>
        {/if}
      </span>
      {runningCount} / {data.connectors.length} running
      {#if !isExpanded && anyUnhealthy}
        {@render unhealthyChip(
          data.connectors
            .filter(isUnhealthy)
            .map((c) => c.health?.description)
            .filter(Boolean) // Filter out missing and empty descriptions to avoid large gaps in the combined text
            .join('\n\n') || ''
        )}
      {/if}
    </div>
  </td>
  <td class="text-center">
    {#if !isExpanded}
      {@render inputTransactionCell(aggregateTransactionPhase)}
    {/if}
  </td>
{/snippet}

{#snippet viewMultiConnectorCell(data: AggregatedOutputEndpointMetrics, isExpanded: boolean)}
  {@const anyUnhealthy = data.connectors.some(isUnhealthy)}
  {@const anyFatalError = data.connectors.some((c) => c.fatal_error != null)}
  <td>
    <div data-testid="box-multi-connector-summary" class="flex flex-nowrap items-center">
      <span class="mr-1 flex w-10 flex-nowrap justify-end gap-1">
        {#if !isExpanded}
          {@render outputConnectorIcons({
            hasErrors: data.connectors.some(outputHasErrors),
            hasFatalError: anyFatalError
          })}
        {/if}
      </span>
      {data.connectors.length} connectors
      {#if !isExpanded && anyUnhealthy}
        {@render unhealthyChip(
          data.connectors
            .filter(isUnhealthy)
            .map((c) => c.health?.description)
            .filter(Boolean) // Filter out missing and empty descriptions to avoid large gaps in the combined text
            .join('\n\n') || ''
        )}
      {/if}
    </div>
  </td>
{/snippet}

{#snippet metricsTable<EndpointMetrics, Extra extends { io_active: boolean }>(
  maxWidth: string,
  tableData: Map<string, AggregatedMetrics<EndpointMetrics, Extra>>,
  expanded: Set<string>,
  toggle: (r: string) => void,
  connectorNameSnippet: Snippet<
    [AggregatedMetrics<EndpointMetrics, Extra>['connectors'][0], string, boolean?]
  >,
  multiConnectorRelationCell: Snippet<[AggregatedMetrics<EndpointMetrics, Extra>, boolean]>,
  columnHeaders: Snippet,
  metricsCells: Snippet<[EndpointMetrics, boolean | undefined, string?, string?]>,
  testId: string
)}
  <div class="scrollbar w-full overflow-x-auto {maxWidth}" data-testid={testId}>
    <table class="bg-white-dark table h-min rounded text-base">
      <thead>
        {@render columnHeaders()}
      </thead>
      <tbody>
        {#each tableData.entries() as [relation, data]}
          {@const isExpanded = expanded.has(relation)}
          {@const hasMultipleConnectors = data.connectors.length > 1}
          <tr
            data-testid="box-relation-row-{relation}"
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
            {#if hasMultipleConnectors}
              {@render multiConnectorRelationCell(data, isExpanded)}
            {:else if data.connectors.length === 1}
              {@render connectorNameSnippet(data.connectors[0], relation, true)}
            {/if}
            {@render metricsCells(
              data.aggregate.metrics,
              data.connectors.some((c) => c.io_active),
              hasMultipleConnectors ? undefined : relation,
              hasMultipleConnectors ? undefined : data.connectors[0]?.endpointName
            )}
          </tr>
          {#if isExpanded && hasMultipleConnectors}
            {#each data.connectors as connector}
              <tr data-testid="box-connector-row-{connector.endpointName}">
                <td></td>
                {@render connectorNameSnippet(connector, relation, true)}
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
    filteredTables,
    expandedTables,
    toggleTable,
    inputConnectorName,
    tableMultiConnectorCell,
    tableColumnHeaders,
    inputMetricsCells,
    'box-input-tables'
  )}
{/if}
{#if metrics.current.views.size}
  {@render metricsTable(
    'max-w-[1540px]',
    filteredViews,
    expandedViews,
    toggleView,
    outputConnectorName,
    viewMultiConnectorCell,
    viewColumnHeaders,
    outputMetricsCells,
    'box-output-views'
  )}
{/if}
