<script lang="ts" module>
  import type { NodeAttributes } from 'profiler-lib'

  export type MetricsMode = 'overview' | 'node' | 'top-nodes'

  /** The synthetic root "region" node represents the whole circuit (the overview) rather than a
   *  single operator. Reused wherever we need to distinguish overview data from a single node. */
  export function isOverviewAttributes(nodeAttributes: NodeAttributes): boolean {
    return nodeAttributes.title === 'n region'
  }

  /** The node title is built as `${id} ${operation}`; the id (first token) is what `search()`
   *  matches against, so it's the query that links back to the node in the diagram. */
  export function nodeSearchQuery(nodeAttributes: NodeAttributes): string {
    return nodeAttributes.title.split(' ')[0] ?? ''
  }
</script>

<script lang="ts">
  import type { TooltipData } from './ProfilerTooltip.svelte'
  import MetricsDistributionBlock from './metrics/blocks/MetricsDistributionBlock.svelte'
  import { buildBlocks, type RenderableBlock } from './metrics/dispatch'
  import type { LookupCoordinator } from '../functions/lookup'

  interface Props {
    mode: MetricsMode
    tooltipData: TooltipData | null
    /** When true, metrics flagged `advanced` in the profile metadata are included. */
    showAdvanced: boolean
    /** Lookup coordinator; the view registers an imperative handler so each Enter on the
     *  panel's search input re-runs the search even when the query string is unchanged. */
    lookup?: LookupCoordinator
    /** Called when the node title is clicked, to link back to (search for) the node in the
     *  diagram — same effect as the "Search node" input. */
    onSearchNode?: (query: string) => void
  }

  const { mode, tooltipData, showAdvanced, lookup, onSearchNode }: Props = $props()

  const nodeAttributes = $derived(
    tooltipData && 'nodeAttributes' in tooltipData ? tooltipData.nodeAttributes : null
  )
  // Single-node data (a specific operator) as opposed to the whole-circuit overview.
  const isNodeView = $derived(nodeAttributes ? !isOverviewAttributes(nodeAttributes) : false)
  const blocks = $derived<RenderableBlock[]>(
    nodeAttributes ? buildBlocks(nodeAttributes, showAdvanced) : []
  )
  const showAttributesView = $derived(mode === 'overview' || mode === 'node')

  let containerEl: HTMLDivElement | undefined = $state()

  // Container-width-driven column count. A ResizeObserver tracks the scroll container's
  // own width, so the column count reacts to the panel's layout (resizable pane / sidebar
  // changes), not just the viewport.
  let containerWidth = $state(0)
  const TWO_COLUMN_THRESHOLD_PX = 1200
  const useTwoColumns = $derived(containerWidth >= TWO_COLUMN_THRESHOLD_PX)

  $effect(() => {
    if (!containerEl) return
    const observer = new ResizeObserver((entries) => {
      containerWidth = entries[0]?.contentRect.width ?? 0
    })
    observer.observe(containerEl)
    return () => observer.disconnect()
  })

  // Search priorities: block title, then metric label, then metric id. Always returns the
  // *block* to scroll to — metrics in distribution blocks share grid cells with their
  // siblings and don't have a single DOM anchor, so block-level is the reliable target.
  function findMatchingBlockId(query: string): string | null {
    const q = query.trim().toLowerCase()
    if (!q || blocks.length === 0) return null
    for (const b of blocks) {
      if (b.title?.toLowerCase().includes(q)) return b.id
    }
    for (const b of blocks) {
      for (const e of b.entries) {
        if (e.label.toLowerCase().includes(q)) return b.id
      }
    }
    for (const b of blocks) {
      for (const e of b.entries) {
        if (e.row.metric.toLowerCase().includes(q)) return b.id
      }
    }
    return null
  }

  // Imperative handler. Each Enter on the panel's search input calls this directly via the
  // lookup coordinator, so identical queries still re-fire (unlike a reactive `$effect` on a
  // query prop, where Svelte would dedupe equal values).
  function runSearch(query: string) {
    if (!containerEl) return
    const matchId = findMatchingBlockId(query)
    if (!matchId) return
    const el = containerEl.querySelector<HTMLElement>(`[data-block-id="${matchId}"]`)
    el?.scrollIntoView({ block: 'start', behavior: 'smooth' })
  }

  $effect(() => {
    if (!lookup) return
    return lookup.register('Metrics', runSearch)
  })

  const genericTable = $derived(
    tooltipData && 'genericTable' in tooltipData ? tooltipData.genericTable : null
  )
</script>

{#snippet attributesView()}
  {#if !nodeAttributes}
    <div class="flex flex-1 items-center justify-center text-sm text-surface-600-400">
      {#if mode === 'node'}
        Click a node in the graph to see its metrics.
      {:else}
        No profile data loaded.
      {/if}
    </div>
  {:else}
    {#if isNodeView}
      <button
        type="button"
        title="Show this node in the diagram"
        class="mb-3 block cursor-pointer text-left text-base font-semibold text-primary-600-400 hover:underline"
        onclick={() => onSearchNode?.(nodeSearchQuery(nodeAttributes))}
      >{nodeAttributes.title}</button>
    {/if}
    <!-- Two same-width columns once the container is at least TWO_COLUMN_THRESHOLD_PX wide;
         otherwise one column. CSS multi-column flow auto-distributes blocks; the column
         count is driven by the ResizeObserver on the scroll container. -->
    <div class="gap-3" style="column-count: {useTwoColumns ? 2 : 1};">
      {#each blocks as b (b.id)}
        <div class="mb-3 break-inside-avoid">
          <MetricsDistributionBlock id={b.id} title={b.title} entries={b.entries} />
        </div>
      {/each}
    </div>
  {/if}
{/snippet}

{#snippet topNodesView()}
  {#if !genericTable}
    <div class="flex flex-1 items-center justify-center text-sm text-surface-600-400">
      No top-nodes data — select a metric to compute.
    </div>
  {:else}
    <div class="rounded-container bg-white-dark p-4 shadow-sm">
      <h3 class="mb-3 text-base font-semibold text-surface-900-100">{genericTable.header}</h3>
      <table class="w-full border-collapse text-sm">
        <thead>
          <tr class="text-left text-xs font-medium uppercase tracking-wide text-surface-600-400">
            {#each genericTable.columns as col}
              <th class="px-2 py-1">{col}</th>
            {/each}
          </tr>
        </thead>
        <tbody>
          {#each genericTable.rows as row}
            <tr class="border-t border-surface-200-800">
              <td class="px-2 py-1">
                <button
                  type="button"
                  class="cursor-pointer text-primary-600-400 hover:underline"
                  onclick={() => row.stub.onclick?.()}
                >{row.stub.text}</button>
              </td>
              {#each row.cells as cell}
                <td class="px-2 py-1 text-right tabular-nums">{cell.text}</td>
                <td class="px-2 py-1 text-surface-700-300">{cell.operation}</td>
              {/each}
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
{/snippet}

<div class="absolute inset-0 overflow-auto scrollbar" bind:this={containerEl}>
  {#if showAttributesView}
    {@render attributesView()}
  {:else}
    {@render topNodesView()}
  {/if}
</div>

