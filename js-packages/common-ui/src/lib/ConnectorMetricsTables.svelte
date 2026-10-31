<script lang="ts">
  import { count } from 'common-lib/array'
  import { formatDuration, formatQty } from 'common-lib/format'
  import { humanSize } from 'common-lib/string'
  import type { Snippet } from 'svelte'
  import type { HTMLAttributes } from 'svelte/elements'
  import ClipboardCopyButton from './ClipboardCopyButton.svelte'
  import type {
    AggregatedInputEndpointMetrics,
    AggregatedMetrics,
    AggregatedOutputEndpointMetrics,
    ConnectorErrorFilter,
    ConnectorMetrics,
    InputEndpointMetrics,
    OutputEndpointMetrics
  } from './connectorMetrics'
  import { defaultLatencyColorSpread, latencyColor, latencyColorScale } from './latencyColor'
  import Popover from './Popover.svelte'
  import SegmentedControl from './SegmentedControl.svelte'
  import { statusChipClass, statusCounterClass, statusToneColors } from './statusTone'
  import Tooltip from './Tooltip.svelte'

  let {
    metrics,
    onConnectorSelect
  }: {
    metrics: { current: ConnectorMetrics }
    /**
     * Shows the errors of a connector. If it is not set, the error counts and icons are not
     * clickable.
     */
    onConnectorSelect?: (
      relationName: string,
      connectorName: string,
      direction: 'input' | 'output',
      filter: ConnectorErrorFilter
    ) => void
  } = $props()

  // Calculated from individual connectors, so that a relation's color
  // stays comparable to the connectors it summarizes.
  const inputLatencyScale = $derived(
    latencyColorScale(
      [...metrics.current.tables.values()].flatMap((data) =>
        data.connectors.map((connector) => connector.metrics.processing_latency_p99_micros)
      ),
      defaultLatencyColorSpread
    )
  )

  type HealthFilter = 'all' | 'unhealthy'
  let tableHealthFilter = $state<HealthFilter>('all')
  let viewHealthFilter = $state<HealthFilter>('all')
  const healthFilterModes: HealthFilter[] = ['all', 'unhealthy']

  // Each number column reserves room for the longest value its formatter usually produces,
  // in DM Mono digits plus the cell's 1rem of padding, so columns keep their width as live
  // values change. A wider value or header still widens the column.
  // Columns of one metric family sit close together; each new family starts after the wider
  // `groupGap`, whose padding the first column's width includes.
  const numberWidth = {
    records: 'w-[calc(11ch+1rem)]', // formatQty: "999,999,999"
    bytes: 'w-[calc(10ch+1rem)]', // humanSize: "1023.9 MiB"
    bufferedBytes: 'w-[calc(8ch+1rem)]', // humanSize: "99.9 MiB"; tight so it sits by Buffered records
    count: 'w-[calc(7ch+1rem)]', // formatQty: "999,999"
    // Buffered records: `records` after the 16px `bufferedGap` (24px tighter than `groupGap`).
    bufferedRecords: 'w-[calc(11ch+1.5rem)]',
    groupLatency: 'w-[calc(7ch+3rem)]', // formatDuration: "99.9 ms", starting a family
    groupCount: 'w-[calc(7ch+3rem)]' // `count` starting a family
  }

  // A wider gap before the columns that start a new group: Connectors (also room for the
  // unhealthy icon to hang before the status icons), Transaction, and the first column of each
  // metric family after the first (Buffered, Latency and Parse errors; in Views, Buffered and
  // Encode errors).
  const groupGap = 'pl-10!'
  // Before Buffered records the numbers already reserve empty room, so the gap is 24px tighter.
  const bufferedGap = 'pl-4!'

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

<!-- Points right while collapsed, down while expanded. -->
{#snippet chevron(isExpanded: boolean)}
  <div
    class="fd fd-chevron-down mr-1 inline-block w-4 text-center text-[16px]"
    class:-rotate-90={!isExpanded}
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
      class="fd fd-circle-alert text-[16px] text-error-500 {onErrorClick ? 'cursor-pointer' : ''}"
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
      class="-my-1 {statusChipClass} {statusToneColors.blue.chip}">Started</span
    >
  {:else if transactionPhase === 'committed'}
    <span
      data-testid="box-icon-transaction-committed"
      class="-my-1 {statusChipClass} {statusToneColors.jade.chip}">Ready to commit</span
    >
  {/if}
{/snippet}

{#snippet outputConnectorIcons({
  paused,
  hasErrors,
  hasFatalError,
  onErrorClick
}: {
  paused: boolean | undefined
  hasErrors: boolean
  hasFatalError: boolean
  onErrorClick?: (e: Event) => void
})}
  {#if hasErrors}
    <span
      data-testid="btn-icon-output-errors"
      class="fd {hasFatalError
        ? 'fd-circle-x'
        : 'fd-circle-alert'} text-[16px] text-error-500 {onErrorClick ? 'cursor-pointer' : ''}"
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
  {#if paused}
    <span
      data-testid="box-icon-output-paused"
      class="fd fd-circle-pause text-[16px] text-surface-700-300"
    ></span>
    <Tooltip placement="top">Paused — output is discarded</Tooltip>
  {:else}
    <span
      data-testid="box-icon-output-running"
      class="fd fd-circle-play text-[16px] text-success-500"
    ></span>
    <Tooltip placement="top">Running</Tooltip>
  {/if}
{/snippet}

{#snippet connectorName(name: string)}
  <span class="relative h-[20px] min-w-0 flex-1 overflow-hidden">
    <span class="absolute inset-0 overflow-hidden text-left text-nowrap text-ellipsis" dir="rtl">
      {name}
    </span>
  </span>
{/snippet}

<!-- A warning icon in the slow-latency red; the word "Unhealthy" leads the popover. It hangs in
     the gutter before the status icons (see `groupGap`), so the status icons of every
     row stay in one column. Render it inside a `relative` element that starts with them. -->
{#snippet unhealthyChip(description: string)}
  <span
    data-testid="box-unhealthy-chip"
    role="img"
    aria-label="Unhealthy"
    class="fd fd-triangle-alert absolute top-1/2 right-full mr-1 -translate-y-1/2 cursor-pointer text-[16px] leading-none text-(--latency-slow)"
  ></span>
  <Popover class="z-20 max-w-lg text-[14px] leading-[20px]">
    <div class="flex flex-nowrap items-start gap-4">
      {#if description}
        <span class="min-w-0 flex-1 wrap-break-word whitespace-pre-wrap">
          <span class="font-medium text-(--latency-slow)">Unhealthy</span> · {description}
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
  <td class={groupGap}>
    <div class="flex min-w-0 flex-nowrap items-center">
      <span class="relative mr-1 flex shrink-0 flex-nowrap gap-1">
        {#if showHealthChip && connector.health?.status === 'Unhealthy'}
          {@render unhealthyChip(connector.health.description ?? '')}
        {/if}
        {@render inputConnectorIcons({
          paused: connector.paused,
          hasErrors: inputHasErrors(connector),
          hasFatalError: connector.fatal_error != null,
          barrier: connector.barrier,
          endOfInput: connector.metrics.end_of_input,
          onErrorClick: onConnectorSelect
            ? () => onConnectorSelect(relation, connector.endpointName, 'input', 'all')
            : undefined
        })}
      </span>
      {@render connectorName(connector.endpointName)}
    </div>
  </td>
  <td class={groupGap}>{@render inputTransactionCell(connector.transaction_phase)}</td>
{/snippet}

{#snippet outputConnectorName(
  connector: AggregatedOutputEndpointMetrics['connectors'][0],
  relation: string,
  showHealthChip?: boolean
)}
  <td class={groupGap}>
    <div class="flex min-w-0 flex-nowrap items-center">
      <span class="relative mr-1 flex shrink-0 flex-nowrap gap-1">
        {#if showHealthChip && connector.health?.status === 'Unhealthy'}
          {@render unhealthyChip(connector.health.description ?? '')}
        {/if}
        {@render outputConnectorIcons({
          paused: connector.paused,
          hasErrors: outputHasErrors(connector),
          hasFatalError: connector.fatal_error != null,
          onErrorClick: onConnectorSelect
            ? () => onConnectorSelect(relation, connector.endpointName, 'output', 'all')
            : undefined
        })}
      </span>
      {@render connectorName(connector.endpointName)}
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
    onValueChange={setFilter}
    items={healthFilterModes.map((value) => ({
      value,
      label: value,
      testid: `btn-select-${value}`
    }))}
    size="sm"
    itemTextClass="capitalize"
  >
    {#snippet label(item)}
      <!-- The count shows even at zero, in the neutral tone, so it is always clear how many
           connectors are unhealthy. -->
      {item.value}{#if item.value === 'unhealthy'}<span
          data-testid="box-unhealthy-count"
          class="ml-1 {statusCounterClass} {statusToneColors[
            unhealthyCount > 0 ? 'error' : 'neutral'
          ].chip}">{unhealthyCount}</span
        >{/if}
    {/snippet}
  </SegmentedControl>
{/snippet}

{#snippet relationHeader(label: string, chevronGutter: boolean, relationNames: string[])}
  <th class="w-px text-nowrap">
    <span class:pl-5={chevronGutter}>{label}</span>
    <!-- Invisible copies of every relation name, so the column keeps its width when the health
         filter hides rows. Drawn as generated content, so they aren't text in the page. -->
    <span class="invisible block h-0 overflow-hidden" aria-hidden="true">
      {#each relationNames as name (name)}
        <span
          class="block before:content-[attr(data-name)]"
          class:pl-5={chevronGutter}
          data-name={name}
        ></span>
      {/each}
    </span>
  </th>
{/snippet}

{#snippet connectorsHeader(
  unhealthyCount: number,
  filter: HealthFilter,
  setFilter: (v: HealthFilter) => void
)}
  <!-- The only column without a width, so it takes the table's spare room. -->
  <th class="min-w-52 {groupGap}">
    <div class="flex items-center gap-2.5">
      <span>Connectors</span>
      {@render connectorHealthFilterControl(unhealthyCount, filter, setFilter)}
    </div>
  </th>
{/snippet}

<!-- A metric's family sits above its unit, so every column keeps its own header. -->
{#snippet metricHeader(family: string, unit: string, extraClass = '')}
  <th class="text-end! {extraClass}">{family}<br />{unit}</th>
{/snippet}

{#snippet tableColumnHeaders(chevronGutter: boolean, relationNames: string[])}
  <tr>
    {@render relationHeader('Table', chevronGutter, relationNames)}
    {@render connectorsHeader(
      unhealthyInputCount,
      tableHealthFilter,
      (v) => (tableHealthFilter = v)
    )}
    <!-- Wide enough for the "Ready to commit" chip, so the column doesn't jump when one appears,
         plus the `groupGap` padding before it. -->
    <th class="w-46 {groupGap}">Transaction</th>
    {@render metricHeader('Ingested', 'records')}
    {@render metricHeader('Ingested', 'bytes')}
    {@render metricHeader('Buffered', 'records', bufferedGap)}
    {@render metricHeader('Buffered', 'bytes')}
    <th class="text-end! {groupGap}">
      <!-- Two lines like the other headers, with the icon right after "p99". One element, so
           hovering either line or the icon opens the tooltip. -->
      <span class="inline-flex cursor-help flex-col items-end">
        <span>Latency</span>
        <span class="inline-flex items-center gap-1">
          p99
          <span
            data-testid="box-icon-latency-help"
            class="fd fd-circle-help text-[16px] leading-none text-surface-600-400"
          ></span>
        </span>
      </span>
      <Tooltip placement="top" class="max-w-sm text-start text-wrap"
        >99th percentile time from ingesting a batch of records to the circuit finishing processing
        it, taken over the last 10 minutes. Colored relative to the other connectors — red marks the
        slowest.</Tooltip
      >
    </th>
    <th class="text-end! {groupGap}">Parse <br />errors</th>
    <th class="text-end!">Transport <br />errors</th>
  </tr>
{/snippet}

<!-- A byte size with its unit in the muted header grey, so the number reads first. -->
{#snippet bytes(size: number)}
  {@const text = humanSize(size)}
  {@const space = text.lastIndexOf(' ')}
  {text.slice(0, space)} <span class="text-surface-400-600">{text.slice(space + 1)}</span>
{/snippet}

{#snippet inputMetricsCells(
  m: InputEndpointMetrics,
  ioActive?: boolean,
  relation?: string,
  connectorEndpointName?: string
)}
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.records}"
    ><span class={ioActive ? 'text-success-600-400' : ''}>{formatQty(m.total_records)}</span></td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.bytes}"
    ><span class={ioActive ? 'text-success-600-400' : ''}>{@render bytes(m.total_bytes)}</span></td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.bufferedRecords} {bufferedGap}"
    >{formatQty(m.buffered_records)}</td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.bufferedBytes}"
    >{@render bytes(m.buffered_bytes)}</td
  >
  <td
    class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.groupLatency} {groupGap}"
    data-testid="box-input-latency-p99"
  >
    {#if typeof m.processing_latency_p99_micros === 'number'}
      <!-- A monospace space is as wide as a digit; narrow it so the unit sits next to the value. -->
      <span
        class="[word-spacing:-0.3em]"
        style:color={latencyColor(m.processing_latency_p99_micros, inputLatencyScale)}
      >
        {formatDuration(m.processing_latency_p99_micros)}
      </span>
    {:else}
      <span class="text-surface-500">—</span>
    {/if}
  </td>
  <td
    class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.groupCount} {groupGap} {m.num_parse_errors >
    0
      ? 'text-error-500'
      : ''}"
  >
    {#if m.num_parse_errors > 0 && relation && connectorEndpointName && onConnectorSelect}
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
    class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.count} {m.num_transport_errors >
    0
      ? 'text-error-500'
      : ''}"
  >
    {#if m.num_transport_errors > 0 && relation && connectorEndpointName && onConnectorSelect}
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

{#snippet viewColumnHeaders(chevronGutter: boolean, relationNames: string[])}
  <tr>
    {@render relationHeader('View', chevronGutter, relationNames)}
    {@render connectorsHeader(
      unhealthyOutputCount,
      viewHealthFilter,
      (v) => (viewHealthFilter = v)
    )}
    {@render metricHeader('Transmitted', 'records')}
    {@render metricHeader('Transmitted', 'in batch')}
    {@render metricHeader('Transmitted', 'bytes')}
    {@render metricHeader('Buffered', 'records', bufferedGap)}
    {@render metricHeader('Buffered', 'batches')}
    {@render metricHeader('Queued', 'records')}
    {@render metricHeader('Queued', 'batches')}
    <th class="text-end! {groupGap}">Encode <br />errors</th>
    <th class="text-end!">Transport <br />errors</th>
  </tr>
{/snippet}

{#snippet outputMetricsCells(
  m: OutputEndpointMetrics,
  ioActive?: boolean,
  relation?: string,
  connectorEndpointName?: string
)}
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.records}"
    ><span class={ioActive ? 'text-success-600-400' : ''}>{formatQty(m.transmitted_records)}</span
    ></td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.records}"
    ><span class={ioActive ? 'text-success-600-400' : ''}
      >{m.batch_records_written != null ? formatQty(m.batch_records_written) : '—'}</span
    ></td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.bytes}"
    ><span class={ioActive ? 'text-success-600-400' : ''}>{@render bytes(m.transmitted_bytes)}</span
    ></td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.bufferedRecords} {bufferedGap}"
    >{formatQty(m.buffered_records)}</td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.count}"
    >{formatQty(m.buffered_batches)}</td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.records}"
    >{formatQty(m.queued_records)}</td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.count}"
    >{formatQty(m.queued_batches)}</td
  >
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.groupCount} {groupGap}">
    {#if m.num_encode_errors > 0 && relation && connectorEndpointName && onConnectorSelect}
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
  <td class="text-end font-dm-mono text-[14px] text-nowrap {numberWidth.count}">
    {#if m.num_transport_errors > 0 && relation && connectorEndpointName && onConnectorSelect}
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
  {@const runningCount = data.connectors.filter(
    (c) => c.paused === false && c.metrics.end_of_input === false
  ).length}
  {@const anyErrors = data.connectors.some(inputHasErrors)}
  {@const anyFatalError = data.connectors.some((c) => c.fatal_error != null)}
  {@const anyBarrier = data.connectors.some((c) => c.barrier === true)}
  {@const anyUnhealthy = data.connectors.some(isUnhealthy)}
  {@const aggregateTransactionPhase = data.connectors.some((c) => c.transaction_phase === 'started')
    ? 'started'
    : data.connectors.some((c) => c.transaction_phase === 'committed')
      ? 'committed'
      : undefined}
  <td class={groupGap}>
    <div data-testid="box-multi-connector-summary" class="flex flex-nowrap items-center">
      <span class="relative mr-1 flex shrink-0 flex-nowrap gap-1">
        {#if !isExpanded && anyUnhealthy}
          {@render unhealthyChip(
            data.connectors
              .filter(isUnhealthy)
              .map((c) => c.health?.description)
              .filter(Boolean) // Filter out missing and empty descriptions to avoid large gaps in the combined text
              .join('\n\n') || ''
          )}
        {/if}
        {#if !isExpanded}
          {@render inputConnectorIcons({
            paused: runningCount > 0 ? false : true,
            hasErrors: anyErrors,
            hasFatalError: anyFatalError,
            barrier: anyBarrier,
            endOfInput: data.aggregate.metrics.end_of_input
          })}
        {/if}
      </span>
      {runningCount} / {data.connectors.length} running
    </div>
  </td>
  <td class={groupGap}>
    {#if !isExpanded}
      {@render inputTransactionCell(aggregateTransactionPhase)}
    {/if}
  </td>
{/snippet}

{#snippet viewMultiConnectorCell(data: AggregatedOutputEndpointMetrics, isExpanded: boolean)}
  {@const anyUnhealthy = data.connectors.some(isUnhealthy)}
  {@const anyFatalError = data.connectors.some((c) => c.fatal_error != null)}
  <!-- A pipeline that predates pausable output connectors reports no `paused`
       field; such a connector is running. -->
  {@const runningCount = data.connectors.filter((c) => !c.paused).length}
  <td class={groupGap}>
    <div data-testid="box-multi-connector-summary" class="flex flex-nowrap items-center">
      <span class="relative mr-1 flex shrink-0 flex-nowrap gap-1">
        {#if !isExpanded && anyUnhealthy}
          {@render unhealthyChip(
            data.connectors
              .filter(isUnhealthy)
              .map((c) => c.health?.description)
              .filter(Boolean) // Filter out missing and empty descriptions to avoid large gaps in the combined text
              .join('\n\n') || ''
          )}
        {/if}
        {#if !isExpanded}
          {@render outputConnectorIcons({
            paused: runningCount === 0,
            hasErrors: data.connectors.some(outputHasErrors),
            hasFatalError: anyFatalError
          })}
        {/if}
      </span>
      {runningCount} / {data.connectors.length} running
    </div>
  </td>
{/snippet}

{#snippet metricsTable<EndpointMetrics, Extra extends { io_active: boolean }>(
  maxWidth: string,
  tableData: Map<string, AggregatedMetrics<EndpointMetrics, Extra>>,
  // Every relation, before the health filter, so the relation column's width doesn't follow it.
  allData: Map<string, AggregatedMetrics<EndpointMetrics, Extra>>,
  expanded: Set<string>,
  toggle: (r: string) => void,
  connectorNameSnippet: Snippet<
    [AggregatedMetrics<EndpointMetrics, Extra>['connectors'][0], string, boolean?]
  >,
  multiConnectorRelationCell: Snippet<[AggregatedMetrics<EndpointMetrics, Extra>, boolean]>,
  columnHeaders: Snippet<[boolean, string[]]>,
  metricsCells: Snippet<[EndpointMetrics, boolean | undefined, string?, string?]>,
  testId: string
)}
  {@const chevronGutter = [...allData.values()].some((data) => data.connectors.length > 1)}
  <div class="latency-colors w-full {maxWidth}" data-testid={testId}>
    <div class="scrollbar w-full overflow-x-auto">
      <div class="bg-white-dark w-max min-w-full overflow-hidden rounded">
        <table class="metrics-table table h-min w-full text-[14px] leading-[20px]">
          <thead>
            {@render columnHeaders(chevronGutter, [...allData.keys()])}
          </thead>
          <tbody>
            {#each tableData.entries() as [relation, data]}
              {@const isExpanded = expanded.has(relation)}
              {@const hasMultipleConnectors = data.connectors.length > 1}
              <!-- The page's own background is surface-50 (light) and surface-950 (dark), so the
                   hover uses a shade between the table and the page in both modes. -->
              <tr
                data-testid="box-relation-row-{relation}"
                class={hasMultipleConnectors
                  ? 'cursor-pointer hover:bg-surface-100/60 dark:hover:bg-surface-950/50'
                  : ''}
                onclick={() => hasMultipleConnectors && toggle(relation)}
              >
                <td class="font-medium">
                  <div class="text-nowrap">
                    {#if hasMultipleConnectors}
                      {@render chevron(isExpanded)}
                    {:else if chevronGutter}
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
    </div>
  </div>
{/snippet}

{#if metrics.current.tables.size}
  {@render metricsTable(
    'max-w-[1540px]',
    filteredTables,
    metrics.current.tables,
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
    metrics.current.views,
    expandedViews,
    toggleView,
    outputConnectorName,
    viewMultiConnectorCell,
    viewColumnHeaders,
    outputMetricsCells,
    'box-output-views'
  )}
{/if}

<style>
  /* Header cells fit two 20px lines plus padding, in 14px type like the rows. The header
     line and the row lines share one tone. */
  .metrics-table :global(th) {
    padding: 0.5rem;
    height: 56px;
    vertical-align: middle;
    font-size: 0.875rem;
    line-height: 1.25rem;
    font-weight: 500;
    text-align: start;
  }
  .metrics-table :global(td) {
    padding-inline: 0.5rem;
  }
  .metrics-table :global(thead),
  .metrics-table :global(tr) {
    border-color: var(--color-surface-100);
  }
  :global(.dark) .metrics-table :global(thead),
  :global(.dark) .metrics-table :global(tr) {
    border-color: var(--color-surface-900);
  }

  /* Endpoints of the latency column's color scale, consumed by `latencyColor()`
     through the inherited custom-property cascade. Skeleton's dual-tone
     `surface-950-50` variable cannot be used directly: its nested
     `light-dark()` resolves to `transparent` inside `color-mix()`. So the two
     tones are bound separately and switched on `.dark`, which the root layout
     sets on the `html` element. */
  .latency-colors {
    --latency-fast: var(--color-surface-950);
    --latency-slow: var(--color-error-500);
  }
  :global(.dark) .latency-colors {
    --latency-fast: var(--color-surface-50);
    /* A lighter red, so the slowest latency doesn't glare on the dark background. */
    --latency-slow: var(--color-error-300);
  }
</style>
