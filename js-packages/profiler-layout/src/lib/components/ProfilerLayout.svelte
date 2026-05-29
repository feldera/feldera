<script lang="ts">
  import { Select } from 'common-ui'
  import type {
    Dataflow,
    JsonProfiles,
    MetricOption,
    ProfilerCallbacks,
    SourcePositionRange,
    WorkerOption
  } from 'profiler-lib'
  import { default as ProfilerDiagram } from './ProfilerDiagram.svelte'
  import type { TooltipData } from './ProfilerTooltip.svelte'
  import ProfilerTooltip from './ProfilerTooltip.svelte'

  interface Props {
    /** Profile data from the pipeline manager */
    profileData: JsonProfiles
    /** Dataflow graph data from the SQL compiler */
    dataflowData: Dataflow | undefined
    /** Lines of user SQL code; may be missing */
    programCode: string[] | undefined
    /** Optional class for styling the toolbar container */
    toolbarClass?: string
    /** Optional class for styling the diagram container */
    diagramClass?: string
    /** Snippet for toolbar start (Load Profile button and snapshot selector) */
    toolbarStart?: import('svelte').Snippet
    /** Called to highlight the range of code corresponding to the selected diagram node */
    onHighlightSourceRanges?: (sourceRanges: SourcePositionRange[]) => void
  }

  const {
    profileData,
    dataflowData,
    programCode,
    toolbarClass,
    diagramClass,
    toolbarStart,
    onHighlightSourceRanges
  }: Props = $props()

  // UI state managed by this layout
  let tooltipData: TooltipData | null = $state(null)
  let tooltipSticky = $state(false)
  let metrics: MetricOption[] = $state([])
  let selectedMetricId = $state('')
  let workers: WorkerOption[] = $state([])
  let message = $state('')
  let error = $state('')

  // Reference to ProfilerDiagram for calling methods
  let profilerDiagram: ProfilerDiagram | undefined = $state()

  // Reference to ProfilerTooltip for calling methods
  let profilerTooltip: ProfilerTooltip = $state(undefined!)

  // Callbacks for profiler-lib
  const callbacks: ProfilerCallbacks = {
    displayNodeAttributes: (data, isSticky) => {
      tooltipData = data.match({ some: (v) => ({ nodeAttributes: v }), none: () => null })
      tooltipSticky = isSticky
    },
    displayTopNodes(data, isSticky) {
      profilerTooltip.resetScroll()
      tooltipData = data.match({
        some: (topNodes) => ({
          genericTable: {
            header: `Nodes with highest values for the metric "${selectedMetricId}"`,
            columns: ['Node', 'Value', 'Operation'],
            rows: topNodes.map((n) => ({
              stub: { text: n.nodeId, onclick: () => profilerDiagram?.search(n.nodeId) },
              cells: [
                {
                  text: n.label,
                  operation: n.operation,
                  normalizedValue: n.normalizedValue
                }
              ]
            }))
          }
        }),
        none: () => null
      })
      tooltipSticky = isSticky
    },
    onMetricsChanged: (newMetrics: MetricOption[], newSelectedMetricId: string) => {
      metrics = newMetrics
      selectedMetricId = newSelectedMetricId
    },
    onWorkersChanged: (newWorkers: WorkerOption[]) => {
      // Transpose the list of workers to display it as a 2-row CSS grid
      const halfLen = Math.round(newWorkers.length / 2)
      workers = newWorkers.map((_, i) => newWorkers[(i >> 1) + (i % 2) * halfLen])
    },
    displayMessage: (msg) => {
      message = msg.unwrapOr('')
    },
    onError: (err: string) => {
      error = err
    },
    onNodeDoubleClick: handleNodeDoubleClick
  }

  // Handle metric selection change
  $effect(() => {
    profilerDiagram?.selectMetric(selectedMetricId)
  })

  // Handle worker checkbox change
  function handleWorkerChange(workerId: string) {
    profilerDiagram?.toggleWorker(workerId)
  }

  // Handle toggle all workers
  function handleToggleAllWorkers() {
    profilerDiagram?.toggleAllWorkers()
  }

  // Search query state
  let searchQuery = $state('')

  // Handle search
  function handleSearch() {
    if (searchQuery) {
      profilerDiagram?.search(searchQuery)
    }
  }

  // Handle leaf node double-click to navigate to SQL source position
  function handleNodeDoubleClick(nodeId: string, type: 'leaf' | string) {
    if (type !== 'leaf') {
      return
    }

    const profile = profilerDiagram?.getProfile()
    if (!profile) {
      return
    }

    const sourceRanges = profile.getSourceRanges(nodeId)
    if (sourceRanges.length === 0) {
      return
    }

    onHighlightSourceRanges?.(sourceRanges)
  }

  // Public method for search (called by parent)
  export function search(query: string): void {
    profilerDiagram?.search(query)
  }

  // Export state for parent to render controls
  export { metrics, selectedMetricId, workers, message, error }
  export { handleWorkerChange, handleToggleAllWorkers }
</script>

{#snippet pseudoNode({text, ...props}: {onmouseenter: () => void, onmouseleave: () => void, onclick: () => void, text: string})}
  <button
    class="cursor-default rounded-base border border-black bg-white px-2 text-black outline-none"
    {...props}
  >
    {text}
  </button>
{/snippet}

<!-- Toolbar with controls -->
<div class="flex flex-wrap items-center gap-2 {toolbarClass || ''}">
  <!-- Toolbar start snippet (Load Profile and Snapshot) -->
  <!-- <div class="toolbar-start"> -->
  {@render toolbarStart?.()}
  <!-- </div> -->

  <!-- Toolbar end (Metrics, Workers, Search) -->
  <div class="ml-auto flex flex-wrap items-center gap-2">
    <!-- Metric Selector -->
    <label class="flex items-center gap-2 text-sm">
      <span class="text-surface-600-400">Metric:</span>
      <Select bind:value={selectedMetricId}>
        {#each metrics as metric (metric.id)}
          <option value={metric.id}>{metric.label}</option>
        {/each}
      </Select>
    </label>

    {@render pseudoNode({
      onmouseenter: () => profilerDiagram?.showGlobalMetrics(),
      onmouseleave: () => profilerDiagram?.hideNodeAttributes(),
      onclick: () => profilerDiagram?.showGlobalMetrics(true),
      text: 'overall metrics'
    })}

    {@render pseudoNode({
      onmouseenter: () => {
        profilerDiagram?.showTopNodes()
      },
      onmouseleave: () => {
        profilerDiagram?.hideNodeAttributes()
      },
      onclick: () => {
        profilerDiagram?.showTopNodes(true)
      },
      text: 'top nodes'
    })}

    <div class="vr">
      <hr class="vr" />
    </div>

    <!-- Workers Control -->
    <div class="flex items-center gap-2 text-sm">
      <span class="text-surface-600-400">Workers:</span>
      <button onclick={handleToggleAllWorkers} class="btn bg-surface-100-900! px-2">
        Toggle All
      </button>
      <div class="grid grid-flow-col grid-rows-2 gap-0.5">
        {#each workers as worker (worker.id)}
          <input
            type="checkbox"
            checked={worker.checked}
            onchange={() => handleWorkerChange(worker.id)}
            class="checkbox"
            title={worker.label}
          />
        {/each}
      </div>
    </div>

    <!-- Search Input -->
    <label class="flex items-center gap-2 text-sm">
      <span class="text-surface-600-400">Search:</span>
      <input
        bind:value={searchQuery}
        type="text"
        placeholder="ID or persistent ID"
        title="Search for node by ID or a substring of a persistent ID"
        onkeydown={(e) => e.key === 'Enter' && handleSearch()}
        class="input w-32 text-sm"
      />
    </label>
  </div>
</div>

<div class="relative h-full w-full">
  <div class="profiler-layout {diagramClass || ''}">
    {#if error}
      <div class="error-banner" role="alert">
        <strong>Error loading profiler:</strong>
        {error}
      </div>
    {/if}

    <!-- Diagram container with overlays -->
    <div class="profiler-diagram-container">
      <!-- ProfilerDiagram renders the graph and proxies profiler-lib methods -->
      <ProfilerDiagram
        bind:this={profilerDiagram}
        {profileData}
        {dataflowData}
        {programCode}
        {callbacks}
      />

      <!-- Overlay menus (positioned on top of graph) -->
      <div class="profiler-menus">
        <!-- Navigator minimap (rendered by ProfilerDiagram) -->

        <!-- Message container -->
        {#if message}
          <div class="profiler-message">
            {message}
          </div>
        {/if}

        <!-- Error container -->
        {#if error}
          <div class="profiler-error">
            {error}
          </div>
        {/if}
      </div>

      <!-- Tooltip container (positioned in top-right) -->
      <ProfilerTooltip bind:this={profilerTooltip} value={tooltipData} sticky={tooltipSticky}></ProfilerTooltip>
    </div>
  </div>
</div>
