<script lang="ts" module>
  import type { NodeAttributes } from 'profiler-lib'

  export type MetricsMode = 'overview' | 'node' | 'top-nodes'

  /** Node identity attributes shown beside the node title, in display order. */
  const idAttributes = [
    { key: 'parent', label: 'parent ID' },
    { key: 'persistentId', label: 'persistent ID' }
  ]

  /** The toplevel node represents the whole circuit (the overview) rather than a single operator.
   *  `rootNodeId` is the loaded profile's actual toplevel id; while it is `undefined` (no profile
   *  yet) nothing counts as the overview. */
  export function isOverviewAttributes(
    nodeAttributes: NodeAttributes,
    rootNodeId: string | undefined
  ): boolean {
    return rootNodeId !== undefined && nodeAttributes.nodeId === rootNodeId
  }

  /** The node id is what `search()` matches against, so it's the query that links back to the
   *  node in the diagram. */
  export function nodeSearchQuery(nodeAttributes: NodeAttributes): string {
    return nodeAttributes.nodeId
  }
</script>

<script lang="ts">
  import type { TooltipData } from './ProfilerTooltip.svelte'
  import MetricKindBlock from './metrics/blocks/MetricKindBlock.svelte'
  import MetricsDistributionBlock from './metrics/blocks/MetricsDistributionBlock.svelte'
  import { buildCacheTiles, isCacheFamilyMetric } from './metrics/cache'
  import CacheTile from './metrics/parts/cache/CacheTile.svelte'
  import { buildBlocks, type MetricGroup, type RenderableBlock } from './metrics/dispatch'
  import { isCardKind } from './metrics/kind'
  import type { LookupCoordinator } from '../functions/lookup'

  interface Props {
    mode: MetricsMode
    tooltipData: TooltipData | null
    /** The loaded profile's toplevel node id, used to recognise overview data. */
    rootNodeId: string | undefined
    /** When true, metrics flagged `advanced` in the profile metadata are included. */
    showAdvanced: boolean
    /** Lookup coordinator; the view registers an imperative handler so each Enter on the
     *  panel's search input re-runs the search even when the query string is unchanged. */
    lookup?: LookupCoordinator
    /** Called when the node title is clicked, to link back to (search for) the node in the
     *  diagram — same effect as the "Search node" input. */
    onSearchNode?: (query: string) => void
  }

  const { mode, tooltipData, rootNodeId, showAdvanced, lookup, onSearchNode }: Props = $props()

  const nodeAttributes = $derived(
    tooltipData && 'nodeAttributes' in tooltipData ? tooltipData.nodeAttributes : null
  )
  // Single-node data (a specific operator) as opposed to the whole-circuit overview.
  const isNodeView = $derived(
    nodeAttributes ? !isOverviewAttributes(nodeAttributes, rootNodeId) : false
  )
  const identityRows = $derived(
    nodeAttributes && isNodeView
      ? idAttributes.flatMap((r) => {
          const value = nodeAttributes.attributes.get(r.key)
          return value ? [{ ...r, value }] : []
        })
      : []
  )
  const blocks = $derived<RenderableBlock[]>(
    nodeAttributes ? buildBlocks(nodeAttributes, showAdvanced) : []
  )
  // Within a category, bar-chart kinds go to the distribution grid; other kinds each render as
  // their own card. Cache-family metrics are subsumed into composite CacheTiles, so they are
  // excluded from both. Splitting here keeps MetricsDistributionBlock bar-chart-only.
  const barGroups = (block: RenderableBlock): MetricGroup[] =>
    block.entries.filter((g) => !isCardKind(g.kind) && !isCacheFamilyMetric(g.baseId))
  const cardGroups = (block: RenderableBlock): MetricGroup[] =>
    block.entries.filter((g) => isCardKind(g.kind) && !isCacheFamilyMetric(g.baseId))
  const cacheTiles = (block: RenderableBlock) => buildCacheTiles(block.entries)
  // A category renders a header only when it has at least one visible tile (after filtering).
  const hasContent = (block: RenderableBlock): boolean =>
    barGroups(block).length > 0 || cardGroups(block).length > 0 || cacheTiles(block).length > 0
  const showAttributesView = $derived(mode === 'overview' || mode === 'node')

  // Per-category collapse state, keyed by block id; an absent entry means expanded.
  let collapsed = $state<Record<string, boolean>>({})
  const toggleCategory = (id: string) => {
    collapsed[id] = !collapsed[id]
  }

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
        if (e.baseId.toLowerCase().includes(q)) return b.id
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
    // Expand the target category so the matched metric is visible after scrolling.
    collapsed[matchId] = false
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
      <div class="mb-3 flex flex-wrap items-baseline gap-x-3 gap-y-1 text-base">
        <button
          type="button"
          title="Show this node in the diagram"
          class="cursor-pointer text-left font-semibold text-primary-600-400 hover:underline"
          onclick={() => onSearchNode?.(nodeSearchQuery(nodeAttributes))}
        >{nodeAttributes.title}</button>
        {#each identityRows as row (row.key)}
          <span class="text-surface-800-200">
            <span class="font-medium">{row.label}:</span>
            <span class="break-all font-mono">{row.value}</span>
          </span>
        {/each}
      </div>
    {/if}
    <!-- One collapsible section per metrics category. The category name sits on the page
         background with a chevron aligned right that collapses the whole category. Within an
         expanded category, tiles flow across two same-width columns once the container is at
         least TWO_COLUMN_THRESHOLD_PX wide (CSS multi-column, driven by the ResizeObserver),
         otherwise one column. -->
    <div class="metrics-theme">
      {#each blocks as b (b.id)}
        {#if hasContent(b)}
          <section class="mb-4" data-block-id={b.id}>
            <button
              type="button"
              class="mb-2 flex w-full items-center justify-between gap-3 text-left"
              aria-expanded={!collapsed[b.id]}
              onclick={() => toggleCategory(b.id)}
            >
              <span class="text-xl font-semibold text-surface-900-100">{b.title}</span>
              <span
                class="fd fd-chevron-down chevron shrink-0 text-[20px] text-surface-600-400"
                class:rotate-180={!collapsed[b.id]}
                aria-hidden="true"
              ></span>
            </button>
            {#if !collapsed[b.id]}
              <div style="column-count: {useTwoColumns ? 2 : 1};">
                {#if barGroups(b).length > 0}
                  <div class="mb-3 break-inside-avoid">
                    <MetricsDistributionBlock id={`${b.id}-dist`} title={b.title} entries={barGroups(b)} />
                  </div>
                {/if}
                {#each cardGroups(b) as g (g.baseId)}
                  <div class="mb-3 break-inside-avoid">
                    <MetricKindBlock id={`${b.id}-${g.baseId}`} group={g} />
                  </div>
                {/each}
                {#each cacheTiles(b) as tile (tile.prefix)}
                  <div class="mb-3 break-inside-avoid">
                    <CacheTile
                      id={`${b.id}-cache-${tile.prefix}`}
                      title={tile.title}
                      hits={tile.hits}
                      misses={tile.misses}
                      hitRate={tile.hitRate}
                    />
                  </div>
                {/each}
              </div>
            {/if}
          </section>
        {/if}
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

<style>
  /* Shared theme tokens for all metric block visuals, defined once on the container so both
     block types (bar-chart grid and per-kind cards) and their children inherit them via the
     custom-property cascade. We use Skeleton's single-tone vars (defined under [data-theme=...]
     and inherited reliably) and switch on `.dark` / `body.dark` ourselves, instead of relying
     on Skeleton's dual-tone `*-200-800` vars inside color-mix() — which resolved to transparent
     in this setup (cascade re-parsing of the dual-tone value doesn't reach the data-theme
     scope in some browsers). Kept in a <style> block so `:global(.dark)` and `:global(body.dark)`
     can both target the same vars; Tailwind's `dark:` variant alone doesn't cover the
     `body.dark` form used elsewhere in this app. */
  .chevron {
    display: inline-block;
    transition: transform 200ms ease;
  }
  .metrics-theme {
    --bar-low: var(--color-surface-100);
    --bar-high: var(--color-error-300);
    --skew-low: var(--color-surface-600);
    --skew-high: var(--color-error-500);
    --header-bg: white;
    /* Compaction-state chips (K6). */
    --state-none: var(--color-surface-300);
    --state-requested: var(--color-warning-400);
    --state-progress: var(--color-primary-400);
  }
  :global(.dark) .metrics-theme,
  :global(body.dark) .metrics-theme {
    --bar-low: var(--color-surface-900);
    --bar-high: var(--color-error-700);
    --skew-low: var(--color-surface-400);
    --header-bg: var(--color-dark);
    --state-none: var(--color-surface-600);
    --state-requested: var(--color-warning-600);
    --state-progress: var(--color-primary-600);
  }
</style>

