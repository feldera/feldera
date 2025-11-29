<script lang="ts">
  import ProfilerDiagram from './ProfilerDiagram.svelte'
  import type {
    ProfilerCallbacks,
    MetricOption,
    WorkerOption,
    JsonProfiles,
    Dataflow,
    TooltipData
  } from 'profiler-lib'

  interface Props {
    /** Profile data from the pipeline manager */
    profileData: JsonProfiles
    /** Dataflow graph data from the SQL compiler */
    dataflowData: Dataflow
    /** Lines of user SQL code */
    programCode: string[]
    /** Optional class for styling the container */
    class?: string
    /** Snippet for toolbar start (Load Profile button and snapshot selector) */
    toolbarStart?: import('svelte').Snippet
  }

  let { profileData, dataflowData, programCode, class: className, toolbarStart }: Props = $props()

  // UI state managed by this layout
  let tooltipData: TooltipData | null = $state(null)
  let tooltipVisible = $state(false)
  let metrics: MetricOption[] = $state([])
  let selectedMetricId = $state('')
  let workers: WorkerOption[] = $state([])
  let message = $state('')
  let error = $state('')

  // Reference to ProfilerDiagram for calling methods
  let profilerDiagram:
    | {
        selectMetric: (metricId: string) => void
        toggleWorker: (workerId: string) => void
        toggleAllWorkers: () => void
        search: (query: string) => void
      }
    | undefined = $state()

  // Callbacks for profiler-lib
  const callbacks: ProfilerCallbacks = {
    displayNodeAttributes: (data, visible) => {
      tooltipData = data.match({ some: v => v, none: () => null})
      tooltipVisible = visible
    },
    onMetricsChanged: (newMetrics: MetricOption[], newSelectedMetricId: string) => {
      metrics = newMetrics
      selectedMetricId = newSelectedMetricId
    },
    onWorkersChanged: (newWorkers: WorkerOption[]) => {
      workers = newWorkers
    },
    displayMessage: (msg) => {
      message = msg.unwrapOr('')
    },
    onError: (err: string) => {
      error = err
    }
  }

  // Handle metric selection change
  function handleMetricChange(event: Event) {
    const target = event.target as HTMLSelectElement
    profilerDiagram?.selectMetric(target.value)
  }

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

  // Public method for search (called by parent)
  export function search(query: string): void {
    profilerDiagram?.search(query)
  }

  // Export state for parent to render controls
  export { metrics, selectedMetricId, workers, message, error }
  export { handleMetricChange, handleWorkerChange, handleToggleAllWorkers }
</script>

<!-- Toolbar with controls -->
<div class="flex flex-wrap items-center gap-2 pb-2 sm:-mt-2">
  <!-- Toolbar start snippet (Load Profile and Snapshot) -->
  <!-- <div class="toolbar-start"> -->
  {@render toolbarStart?.()}
  <!-- </div> -->

  <!-- Toolbar end (Metrics, Workers, Search) -->
  <div class="ml-auto flex flex-wrap items-center gap-2">
    <!-- Metric Selector -->
    <label class="flex items-center gap-2 text-sm">
      <span class="text-surface-600-400">Metric:</span>
      <select value={selectedMetricId} onchange={handleMetricChange} class="select w-40 text-sm">
        {#each metrics as metric (metric.id)}
          <option value={metric.id}>{metric.label}</option>
        {/each}
      </select>
    </label>

    <!-- Workers Control -->
    <label class="flex items-center gap-2 text-sm">
      <span class="text-surface-600-400">Workers:</span>
      <button onclick={handleToggleAllWorkers} class="btn btn-sm px-2 text-xs !bg-surface-100-900">
        Toggle All
      </button>
      <div class="flex gap-1">
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
    </label>

    <!-- Search Input -->
    <label class="flex items-center gap-2 text-sm">
      <span class="text-surface-600-400">Search:</span>
      <input
        bind:value={searchQuery}
        type="text"
        placeholder="Node ID"
        title="Search for node by ID"
        onkeydown={(e) => e.key === 'Enter' && handleSearch()}
        class="input w-32 text-sm"
      />
    </label>
  </div>
</div>

<div class="relative h-full w-full">
  <div class="profiler-layout {className || ''}">
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
      {#if tooltipVisible && tooltipData}
        <div class="profiler-tooltip-container">
          <div class="profiler-tooltip">
            <table>
              <!-- Header row with worker names -->
              <thead>
                <tr>
                  <th></th>
                  {#each tooltipData.columns as column}
                    <th>{column}</th>
                  {/each}
                </tr>
              </thead>

              <!-- Metric rows -->
              <tbody>
                {#each tooltipData.rows as row}
                  <tr>
                    <td class:current-metric={row.isCurrentMetric}>{row.metric}</td>
                    {#each row.cells as cell}
                      {@const percent = cell.percentile}
                      {@const color = `rgb(255, ${Math.round((255 * (100 - percent)) / 100)}, ${Math.round((255 * (100 - percent)) / 100)})`}
                      <td style:background-color={color} style:color="black" class="text-right">
                        {cell.value}
                      </td>
                    {/each}
                  </tr>
                {/each}

                <!-- Source code row -->
                {#if tooltipData.sources}
                  <tr>
                    <td>sources</td>
                    <td colspan={tooltipData.columns.length} class="source-code"
                      >{tooltipData.sources}</td
                    >
                  </tr>
                {/if}

                <!-- Additional attributes -->
                {#each Array.from(tooltipData.attributes.entries()) as [key, value]}
                  <tr>
                    <td class="whitespace-nowrap">{key}</td>
                    <td colspan={tooltipData.columns.length} class="whitespace-nowrap">{value}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        </div>
      {/if}
    </div>
  </div>
</div>

<style>
  .profiler-layout {
    width: 100%;
    height: 100%;
    display: flex;
    flex-direction: column;
  }

  .profiler-diagram-container {
    flex: 1;
    position: relative;
    overflow: hidden;
  }

  .error-banner {
    background-color: #fee;
    color: #c00;
    padding: 1rem;
    border: 1px solid #fcc;
    border-radius: 4px;
    margin-bottom: 1rem;
    font-family: monospace;
    white-space: pre-wrap;
  }

  .profiler-menus {
    position: absolute;
    top: 0.5rem;
    left: 0.5rem;
    z-index: 1;
    display: flex;
    flex-direction: column;
    align-items: start;
    gap: 0.5rem;
  }

  .profiler-message {
    display: block;
    font-family: monospace;
    background-color: rgba(255, 255, 255, 0.9);
    padding: 0.5rem;
    font-size: 14px;
    border-radius: 4px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  }

  .profiler-error {
    background-color: white;
    color: red;
    display: block;
    font-family: monospace;
    border: 1px solid red;
    border-radius: 4px;
    text-align: center;
    max-width: 350px;
    white-space: pre-wrap;
    padding: 0.5rem;
    box-shadow: 0 2px 4px rgba(255, 0, 0, 0.2);
  }

  .profiler-tooltip-container {
    position: absolute;
    top: 0.5rem;
    right: 0.5rem;
    z-index: 2;
    pointer-events: none;
    max-height: calc(100vh - 1rem);
  }

  /* Tooltip styling */
  .profiler-tooltip {
    background-color: black;
    border-radius: 8px;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.4);
    padding: 0;
    /* Make sure tooltip edges are rounded */
    overflow: clip;
  }

  .profiler-tooltip table {
    background-color: transparent;
    border-collapse: collapse;
    font-size: 12px;
    width: 100%;
  }

  .profiler-tooltip table td,
  .profiler-tooltip table th {
    padding: 2px 10px;
    color: white;
    white-space: nowrap;
  }

  .profiler-tooltip table th {
    background-color: rgb(102, 126, 234);
    font-weight: 600;
    text-align: center;
  }

  .profiler-tooltip table td {
    background-color: black;
  }

  .profiler-tooltip table td.current-metric {
    background-color: blue;
  }

  .profiler-tooltip table td.source-code {
    font-family: monospace;
    white-space: pre-wrap;
    text-align: left;
    min-width: 80ch;
  }

  /* Scrollbar styling */
  .profiler-tooltip-container::-webkit-scrollbar {
    width: 8px;
  }

  .profiler-tooltip-container::-webkit-scrollbar-track {
    background: rgba(0, 0, 0, 0.1);
    border-radius: 4px;
  }

  .profiler-tooltip-container::-webkit-scrollbar-thumb {
    background: rgba(102, 126, 234, 0.5);
    border-radius: 4px;
  }

  .profiler-tooltip-container::-webkit-scrollbar-thumb:hover {
    background: rgba(102, 126, 234, 0.7);
  }
</style>
