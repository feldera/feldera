<script lang="ts">
  import { formatDateTime } from 'common-lib/format'
  import Dayjs from 'dayjs'
  import { fade, slide } from 'svelte/transition'
  import type {
    ConnectorError,
    ConnectorErrorFilter,
    InputEndpointStatus,
    OutputEndpointStatus
  } from './connectorMetrics'
  import { getCaseDependentName } from './felderaRelation'
  import InlineDropdown from './InlineDropdown.svelte'
  import SegmentedControl from './SegmentedControl.svelte'
  import Tooltip from './Tooltip.svelte'

  const inputFilterOptions = ['all', 'parse', 'transport'] as const
  const outputFilterOptions = ['all', 'encode', 'transport'] as const

  let filterOptionLabels: Record<string, string> = {
    transport: 'Transport',
    parse: 'Parse',
    encode: 'Encode',
    all: 'All'
  }

  let tagsFilter: ConnectorErrorFilter = $state('all')

  let {
    relationName,
    connectorName,
    direction,
    filter = 'all',
    status,
    loading = false,
    onClose
  }: {
    relationName: string
    /** Endpoint name, prefixed with the relation name. */
    connectorName: string
    direction: 'input' | 'output'
    filter?: ConnectorErrorFilter
    /** The connector status with its error lists. `null` while it is not available. */
    status: InputEndpointStatus | OutputEndpointStatus | null
    loading?: boolean
    onClose: () => void
  } = $props()

  const filterOptions = $derived(direction === 'input' ? inputFilterOptions : outputFilterOptions)
  const filterItems = $derived(
    filterOptions.map((value) => ({ value, label: filterOptionLabels[value] ?? value }))
  )

  $effect(() => {
    tagsFilter = filter
  })

  const strippedConnectorName = $derived(
    connectorName.slice(getCaseDependentName(relationName).name.length + 1)
  )

  type DisplayItem = { kind: 'error'; error: ConnectorError } | { kind: 'gap'; count: number }

  function withGaps(errors: ConnectorError[]): DisplayItem[] {
    const byTag = new Map<string, ConnectorError[]>()
    for (const e of errors) {
      const k = e.tag ?? 'other'
      const existing = byTag.get(k)
      if (existing) {
        existing.push(e)
      } else {
        byTag.set(k, [e])
      }
    }
    const result: DisplayItem[] = []
    for (const tagErrors of byTag.values()) {
      const sorted = [...tagErrors].sort((a, b) => a.index - b.index)
      for (let i = 0; i < sorted.length; i++) {
        if (i > 0) {
          const gap = sorted[i].index - sorted[i - 1].index - 1
          if (gap > 0) {
            result.push({ kind: 'gap', count: gap })
          }
        }
        result.push({ kind: 'error', error: sorted[i] })
      }
    }
    return result
  }

  function getErrorArrays(
    s: InputEndpointStatus | OutputEndpointStatus
  ): Record<string, ConnectorError[]> {
    if (direction === 'input') {
      return {
        parse: (s as InputEndpointStatus).parse_errors ?? [],
        transport: s.transport_errors ?? []
      }
    }
    return {
      encode: (s as OutputEndpointStatus).encode_errors ?? [],
      transport: s.transport_errors ?? []
    }
  }

  const displayItems = $derived.by(() => {
    if (!status) {
      return [] as DisplayItem[]
    }
    const arrays = getErrorArrays(status)
    const combined =
      tagsFilter === 'all' ? Object.values(arrays).flat() : (arrays[tagsFilter] ?? [])
    return withGaps(combined).reverse()
  })
</script>

<div class="bg-white-dark flex h-full flex-col gap-2 rounded px-4 pt-4">
  <div class="flex items-start justify-between">
    <div>
      <div class="font-medium">{relationName} · {strippedConnectorName}</div>
    </div>
    <button class="fd fd-x text-[20px]" onclick={onClose} aria-label="Close"></button>
  </div>
  <div class="flex flex-nowrap items-center gap-2">
    <SegmentedControl
      value={tagsFilter}
      onValueChange={(v) => (tagsFilter = v as ConnectorErrorFilter)}
      items={filterItems}
      class="-mt-2"
    />
    <span class="text-surface-500">errors</span>
  </div>
  <div class="-mr-4 scrollbar flex-1 overflow-y-auto pr-4">
    {#if loading}
      <div class="p-2 text-surface-500">Loading…</div>
    {:else if displayItems.length === 0}
      <div class="p-2 text-surface-500">
        {#if tagsFilter === 'all'}
          No errors
        {:else}
          No errors of this type
        {/if}
      </div>
    {:else}
      {#each displayItems as item}
        {#if item.kind === 'gap'}
          <div class="px-2 py-1 text-center text-surface-500 italic">
            {#if item.count !== 1}
              {item.count} other errors skipped
            {:else}
              {item.count} other error skipped
            {/if}
          </div>
        {:else}
          <InlineDropdown>
            {#snippet header(isOpen, toggle)}
              <div
                class="flex w-full cursor-pointer items-center gap-2 py-2 pr-2"
                onclick={toggle}
                role="presentation"
              >
                <div
                  class="fd fd-chevron-down text-[20px] transition-transform {isOpen
                    ? 'rotate-180'
                    : ''}"
                ></div>
                <span class="flex-1 overflow-hidden text-nowrap text-ellipsis">
                  {#if !isOpen}
                    <span out:fade={{ duration: 100 }}>{item.error.message}</span>
                  {/if}
                </span>
                {#if status?.fatal_error != null && item.error.message === status.fatal_error}
                  <span class="fd fd-circle-x text-error-500"></span>
                {/if}
                <span class="ml-auto">{Dayjs(item.error.timestamp).format('HH:mm:ss')}</span>
                <Tooltip placement="top">{formatDateTime(Dayjs(item.error.timestamp))}</Tooltip>
              </div>
            {/snippet}
            {#snippet content()}
              <div
                class="mr-16 -mb-7 ml-7 -translate-y-7 pb-2"
                transition:slide={{ duration: 150 }}
              >
                {item.error.message}
              </div>
            {/snippet}
          </InlineDropdown>
        {/if}
      {/each}
    {/if}
  </div>
</div>
