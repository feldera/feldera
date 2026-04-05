<script lang="ts">
  import { SegmentedControl } from '@skeletonlabs/skeleton-svelte'
  import Dayjs from 'dayjs'
  import { fade, slide } from 'svelte/transition'
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import InlineDrawer from '$lib/components/layout/InlineDrawer.svelte'
  import { getCaseDependentName } from '$lib/functions/felderaRelation'
  import { formatDateTime } from '$lib/functions/format'
  import {
    type ConnectorError,
    type InputEndpointStatus,
    type OutputEndpointStatus
  } from '$lib/services/manager'
  import { getInputConnectorStatus, getOutputConnectorStatus } from '$lib/services/pipelineManager'

  export type ConnectorErrorFilter = 'all' | 'parse' | 'transport' | 'encode'

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
    pipelineName,
    relationName,
    connectorName,
    direction,
    filter = 'all',
    open: _open = $bindable(false)
  }: {
    pipelineName: string
    relationName: string
    connectorName: string
    direction: 'input' | 'output'
    filter?: ConnectorErrorFilter
    open: boolean
  } = $props()

  let open = $state(false)
  $effect(() => {
    // Indirection to make sure the sliding animation plays on first open
    open = _open
  })

  const filterOptions = $derived(direction === 'input' ? inputFilterOptions : outputFilterOptions)

  let status: InputEndpointStatus | OutputEndpointStatus | null = $state(null)
  let loading = $state(false)

  $effect(() => {
    if (!open) {
      return
    }
    tagsFilter = filter
  })

  const strippedConnectorName = $derived(
    connectorName.slice(getCaseDependentName(relationName).name.length + 1)
  )

  $effect(() => {
    pipelineName
    relationName
    connectorName
    direction
    loading = true
    status = null

    const request =
      direction === 'input'
        ? getInputConnectorStatus(pipelineName, relationName, strippedConnectorName)
        : getOutputConnectorStatus(pipelineName, relationName, strippedConnectorName)

    request.then((s) => {
      status = s
      loading = false
    })
  })

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
          if (gap > 0) result.push({ kind: 'gap', count: gap })
        }
        result.push({ kind: 'error', error: sorted[i] })
      }
    }
    return result
  }

  function getErrorArrays(
    s: InputEndpointStatus | OutputEndpointStatus
  ): Record<string, ConnectorError[]> {
    if ('parse_errors' in s) {
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
    if (!status) return [] as DisplayItem[]
    const arrays = getErrorArrays(status)
    const combined =
      tagsFilter === 'all' ? Object.values(arrays).flat() : (arrays[tagsFilter] ?? [])
    return withGaps(combined).reverse()
  })
</script>

<!-- TODO: not mobile-friendly -->
<InlineDrawer {open} side="right" width="w-[500px]">
  <div class="bg-white-dark flex h-full flex-col gap-2 rounded p-4">
    <div class="flex items-start justify-between">
      <div>
        <div class="font-medium">{relationName} · {strippedConnectorName}</div>
      </div>
      <button class="fd fd-x text-[20px]" onclick={() => (open = false)} aria-label="Close"
      ></button>
    </div>
    <div class="flex flex-nowrap items-center gap-2">
      <SegmentedControl
        value={tagsFilter}
        onValueChange={(e) => (tagsFilter = e.value as ConnectorErrorFilter)}
      >
        <SegmentedControl.Label />
        <SegmentedControl.Control
          class="-mt-2 w-fit flex-none rounded preset-filled-surface-50-950 p-1"
        >
          <SegmentedControl.Indicator class="bg-white-dark shadow" />
          {#each filterOptions as filter}
            <SegmentedControl.Item value={filter} class="btn h-6 cursor-pointer px-5 text-sm">
              <SegmentedControl.ItemText class="text-surface-950-50">
                {filterOptionLabels[filter]}
              </SegmentedControl.ItemText>
              <SegmentedControl.ItemHiddenInput />
            </SegmentedControl.Item>
          {/each}
        </SegmentedControl.Control>
      </SegmentedControl>
      <span class="text-surface-500">errors</span>
    </div>
    <div class="scrollbar flex-1 overflow-y-auto">
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
</InlineDrawer>
