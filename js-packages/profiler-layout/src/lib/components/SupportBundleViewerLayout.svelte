<script lang="ts">
  import type { ZipItem } from 'but-unzip'
  import {
    isFindShortcut,
    PersistentContent,
    PointerTooltip,
    SearchBar,
    type SearchDirection,
    SegmentedControl,
    Select,
    statusCounterClass,
    statusToneColors,
    type TabSpec,
    TabsPanel,
    usePersistentRect,
    useShortcut
  } from 'common-ui'
  import {
    countConnectorErrors,
    type ConnectorMetrics,
    type ConnectorStatus
  } from 'common-ui/connectorMetrics'
  import type { PipelineMonitorEvent } from 'common-ui/pipelineHealth'
  import { Pane, PaneGroup, PaneResizer } from 'paneforge'
  import type {
    Dataflow,
    DiagramTheme,
    JsonProfiles,
    MetricLevel,
    MetricOption,
    NodeAttributes,
    ProfilerCallbacks,
    SourcePositionRange,
    WorkerOption
  } from 'profiler-lib'
  import { MetricDescriptions } from 'profiler-lib'
  import { createRawSnippet, type Snippet, untrack } from 'svelte'
  import type { TriageResults } from 'triage-types'
  import type { GlobalMetrics } from '../functions/globalMetrics'
  import { createLookupCoordinator, type SearchProgress } from '../functions/lookup'
  import { levelMetrics, metricToSelect } from '../functions/metricLevel'
  import { type AnalysisView, isNodeView, metricsModeOf } from '../functions/metricsMode'
  import type { SourceFile } from '../functions/processZipBundle'
  import { severityLabel, uniqueCategories, uniqueSeverities } from '../functions/triage'
  import MetricSelect from './MetricSelect.svelte'
  import type { MetricsMode } from './MetricsView.svelte'
  import ProfilerDiagram from './ProfilerDiagram.svelte'
  import type { TooltipData } from './ProfilerTooltip.svelte'
  import ProfileTimestampSelector from './ProfileTimestampSelector.svelte'
  import ConfigTab from './tabs/ConfigTab.svelte'
  import ConnectorsTab from './tabs/ConnectorsTab.svelte'
  import HealthTab from './tabs/HealthTab.svelte'
  import IssuesTab from './tabs/IssuesTab.svelte'
  import LogsTab from './tabs/LogsTab.svelte'
  import MetricsTab, { type AnalysisTabProps } from './tabs/MetricsTab.svelte'
  import SourceTab, { type SourceTabProps } from './tabs/SourceTab.svelte'

  const TABS = ['Metrics', 'Connectors', 'Health', 'Logs', 'Config', 'Issues'] as const
  type AnalysisTab = (typeof TABS)[number]

  interface Props {
    /** Parsed circuit profile */
    profileData?: JsonProfiles
    dataflowData: Dataflow | undefined
    logText?: string
    /** Cumulative pipeline-wide metrics from the bundle's `stats.json`; shown in the overview. */
    globalMetrics?: GlobalMetrics
    /** Input and output connector statistics from the bundle's `stats.json`, grouped by table and
     *  view, shown in the Connectors tab. Absent when the bundle has no connector statistics. */
    connectorMetrics?: ConnectorMetrics
    /** The `connectorMetrics` source. The Connectors tab reads the error lists of the connectors
     *  from it. */
    connectorStatus?: ConnectorStatus
    /** Pipeline runtime configuration from the bundle's `pipeline_config.json`; shown in the Config
     *  tab. Absent when the bundle carried no config. */
    runtimeConfig?: unknown
    /** Pipeline monitor events from the bundle's `pipeline_events.json`, shown in the Health tab.
     *  Absent when the bundle has no events file. The tab is disabled when this is absent or empty. */
    pipelineEvents?: PipelineMonitorEvent[]
    /** Pipeline source files from the bundle's `pipeline_config.json`, `program.sql` first. Each
     *  file gets a tab in the code panel. */
    sourceFiles?: SourceFile[]
    triageResults: TriageResults
    /** Palette the circuit diagram is drawn with; follows the application theme */
    theme?: DiagramTheme
    profileFiles: [Date, ZipItem[]][]
    selectedTimestamp: Date | null
    onSelectTimestamp: (timestamp: Date) => void
    sqlPanelFullHeight: boolean
    /** Snippet for the "Load profile" button and timestamp selector in graph-panel header */
    loadProfileControl?: Snippet
    /** Ranges of the SQL that the selected node came from. Their `end` names the last character of
     *  the range, not the position after it. */
    onHighlightSourceRanges?: (ranges: SourcePositionRange[]) => void
    /** Fired when the graph rendering enters or leaves its asynchronous layout phase.
     */
    onRenderingChange?: (rendering: boolean) => void
  }

  let {
    profileData,
    dataflowData,
    logText,
    globalMetrics,
    connectorMetrics,
    connectorStatus,
    runtimeConfig,
    pipelineEvents,
    sourceFiles = [],
    triageResults,
    theme = 'light',
    profileFiles,
    selectedTimestamp,
    onSelectTimestamp,
    sqlPanelFullHeight = $bindable(),
    loadProfileControl,
    onHighlightSourceRanges,
    onRenderingChange
  }: Props = $props()

  const hasProfile = $derived(profileData !== undefined)
  const connectorErrorCount = $derived(
    connectorMetrics ? countConnectorErrors(connectorMetrics) : 0
  )

  const graphPaneDefaultSize = 55
  const graphPaneMinSize = 20
  const belowGraphPaneDefaultSize = 100 - graphPaneDefaultSize

  let profilerDiagram: ProfilerDiagram | undefined = $state()
  // The loaded profile's toplevel node id, so the analysis panel can recognise overview data.
  const diagramRootNodeId = $derived(profilerDiagram?.getProfile()?.rootNodeId)
  let tooltipData: TooltipData | null = $state(null)
  // Most recently inspected operator; the "Node" segment restores it via `setMetricsMode('node')`.
  let lastNodeData: NodeAttributes | null = $state(null)
  let metrics: MetricOption[] = $state([])
  let selectedMetricId = $state('')
  let message = $state('')
  let error = $state('')
  let currentTab = $state<AnalysisTab>('Metrics')
  // Tracked explicitly so the SegmentedControl indicator follows the user's choice instead of a
  // title heuristic on `tooltipData`.
  let analysisView = $state<AnalysisView>('overview')
  const metricsMode = $derived<MetricsMode>(metricsModeOf(analysisView))
  let metricLevel = $state<MetricLevel>('basic')
  // The metric selector lists only the metrics of the selected level.
  const levelMetricOptions = $derived(levelMetrics(metrics, metricLevel))
  // The metric that colors the diagram: `selectedMetricId` when the level lists it, or else a
  // metric of the level. The selector then never holds a metric that it does not list.
  const shownMetricId = $derived(metricToSelect(selectedMetricId, levelMetricOptions))
  let issueSeverityFilter = $state<'all' | 'error' | 'warning' | 'info'>('all')
  let issueCategoryFilter = $state<string>('all')
  const issueCategories = $derived(uniqueCategories(triageResults.results))
  const issueSeverities = $derived(uniqueSeverities(triageResults.results))
  let nodeSearchQuery = $state('')
  let lookupQuery = $state('')
  // The single source of truth for submitted search results, shared by the counter, the match
  // highlight, and the nav buttons:
  //   - `null`         → no active search (nothing highlighted, nothing to navigate).
  //   - `{ total: 0 }` → searched, but the query matched nothing ("No results").
  //   - `total > 0`    → `current` (1-based) of `total` matches.
  // Editing or clearing the query must set this back to `null` so all three reset together.
  let lookupProgress = $state<SearchProgress | null>(null)
  let searchBar: ReturnType<typeof SearchBar> | undefined = $state()
  let nodeSearchInput: HTMLInputElement | undefined = $state()

  // Ctrl/Cmd-F targets the "Search node" input while focused on the circuit diagram, otherwise the
  // active tab's search. Inactive on Connectors, Health and Config (no in-tab search), so the
  // browser's native find popup appears there.
  useShortcut(
    isFindShortcut,
    () => {
      if (profilerDiagram?.isFocused()) {
        nodeSearchInput?.focus()
        nodeSearchInput?.select()
      } else {
        searchBar?.activate()
      }
    },
    () => currentTab !== 'Connectors' && currentTab !== 'Health' && currentTab !== 'Config'
  )
  let highlightRanges: SourcePositionRange[] = $state([])

  // The <ProfilerDiagram> is not rendered inside the graphPanel snippet. It is rendered once, at
  // the root of this component, in a <PersistentContent> overlay with a fixed position. The overlay
  // follows the position and size of an empty placeholder div in the graphPanel snippet. Thus the
  // sqlPanelFullHeight layout toggle moves only the placeholder, and the diagram keeps the
  // Visualizer's state. The placeholder div uses `diagramRect.placeholder` to report its bounding
  // rect to this shared handle.
  const diagramRect = usePersistentRect()

  // The code panel is also rendered once in an overlay at the root, not inside the layout branches.
  // The layout toggle swaps the {#if} branch that renders the panel, and Svelte mounts the new
  // branch before it destroys the old branch. A panel inside each branch would then create a second
  // Monaco model with the same URI, which Monaco rejects. Each layout branch renders only a
  // placeholder.
  const sourcePanelRect = usePersistentRect()

  const lookup = createLookupCoordinator()

  const callbacks: ProfilerCallbacks = {
    // Sticky callbacks only refresh the payload — view state is driven by `onNodeClick` and
    // `setMetricsMode`, never inferred from the data.
    displayNodeAttributes: (data, isSticky) => {
      if (!isSticky) {
        return
      }
      const attrs = data.match({ some: (v) => v, none: () => null })
      untrack(() => {
        if (attrs) {
          tooltipData = { nodeAttributes: attrs }
          // Only cache while we're on a node view, so an `'overview'` round-trip can't
          // overwrite the operator the user actually inspected.
          if (isNodeView(analysisView)) {
            lastNodeData = attrs
          }
        }
      })
    },
    onNodeClick: (nodeId) => {
      // The only path outside `setMetricsMode` that switches to "Node"; `displayNodeAttributes`
      // fires right after with the payload.
      analysisView = { nodeId }
      currentTab = 'Metrics'
      highlightSource(nodeId)
    },
    displayTopNodes(data, _isSticky) {
      tooltipData = data.match({
        some: (topNodes) => ({
          genericTable: {
            header: `Nodes with highest values for "${MetricDescriptions.label(shownMetricId)}"`,
            columns: ['Node', 'Value', 'Operation'],
            rows: topNodes.map((n) => ({
              stub: { text: n.nodeId, onclick: () => profilerDiagram?.search(n.nodeId) },
              cells: [{ text: n.label, operation: n.operation, normalizedValue: n.normalizedValue }]
            }))
          }
        }),
        none: () => null
      })
    },
    onMetricsChanged: (newMetrics, newSelectedMetricId) => {
      metrics = newMetrics
      selectedMetricId = newSelectedMetricId
    },
    displayMessage: (msg) => {
      message = msg.unwrapOr('')
    },
    onError: (err) => {
      error = err
    },
    onRenderingChange: (rendering) => {
      onRenderingChange?.(rendering)
    }
  }

  /** Show the SQL the node was compiled from, selected and scrolled into view in the code panel.
   *  A node compiled from no SQL selects nothing, so the two panels never show different nodes. */
  function highlightSource(nodeId: string) {
    // A region's double click expands it, so a single click is the only gesture left to reach
    // its source. A region carries the positions of everything inside it (see
    // `CircuitProfile.getSourceRanges`), so the same call serves regions and operators alike.
    const ranges = profilerDiagram?.getProfile()?.getSourceRanges(nodeId) ?? []
    highlightRanges = ranges
    if (ranges.length > 0) {
      sourceCurrentTab = 'program.sql'
    }
    onHighlightSourceRanges?.(ranges)
  }

  // Re-color the diagram when the selected metric changes. The selectMetric() call refreshes
  // the current sticky tooltip via the callbacks above, but those only touch tooltipData — so
  // this does not feed back into any effect (no loop).
  $effect(() => {
    profilerDiagram?.selectMetric(shownMetricId)
  })

  // Reset to the overview whenever the loaded bundle changes. `analysisView` is reset first so the
  // SegmentedControl indicator follows the new view, and so the sticky `displayNodeAttributes`
  // that `showGlobalMetrics` triggers sees a non-node view and can't overwrite `lastNodeData`
  // with the overview payload.
  $effect(() => {
    void profileData
    const diagram = profilerDiagram
    queueMicrotask(() => {
      analysisView = 'overview'
      tooltipData = null
      lastNodeData = null
      diagram?.showGlobalMetrics(true)
    })
  })

  /** Switch the analysis panel's view. Writes `analysisView` first so the SegmentedControl
   *  indicator follows the user's pick, then asks the visualizer for the matching payload. */
  function setMetricsMode(mode: MetricsMode) {
    currentTab = 'Metrics'
    if (mode === 'overview') {
      analysisView = 'overview'
      profilerDiagram?.showGlobalMetrics(true)
    } else if (mode === 'top-nodes') {
      analysisView = 'top-nodes'
      profilerDiagram?.showTopNodes(true)
    } else if (mode === 'node' && lastNodeData) {
      analysisView = { nodeId: lastNodeData.nodeId }
      tooltipData = { nodeAttributes: lastNodeData }
    }
  }

  function handleSearch() {
    if (nodeSearchQuery) {
      profilerDiagram?.search(nodeSearchQuery)
    }
  }

  function handleLookup(direction: SearchDirection = 'next') {
    lookupProgress = lookupQuery ? lookup.execute(currentTab, lookupQuery, direction) : null
  }

  // Drop the active tab's submitted results: run its search with an empty query to clear the
  // highlight, then null the progress.
  function resetLookup() {
    lookup.execute(currentTab, '', 'next')
    lookupProgress = null
  }

  $effect(() => {
    currentTab
    lookupQuery = ''
    lookupProgress = null
  })

  // The lines of `program.sql`. `ProfilerDiagram` shows the SQL of each circuit node from these
  // lines when the dataflow has no `sources`, as in a support bundle.
  const programCode = $derived(sourceFiles.find((f) => f.name === 'program.sql')?.code.split('\n'))
  let sourceCurrentTab = $state('program.sql')
  // The file of the selected tab. It is `undefined` when the bundle has no source files.
  // It defaults to the first file of the bundle when the bundle does not have the selected file.
  const currentFile = $derived(
    sourceFiles.find((f) => f.name === sourceCurrentTab) ?? sourceFiles.at(0)
  )
  // All tabs share these props, so only the active tab can be mounted (`keepAlive: false`).
  const sourceTabProps = $derived<SourceTabProps | undefined>(
    currentFile && {
      file: currentFile,
      // Only the SQL program has node ranges to select.
      highlightRanges: currentFile.name === 'program.sql' ? highlightRanges : []
    }
  )
  const sourceTabs = $derived<TabSpec<SourceTabProps>[]>(
    sourceFiles.map((file) => ({
      id: file.name,
      label: createRawSnippet(() => ({ render: () => `<span>${file.name}</span>` })),
      panel: SourceTab,
      keepAlive: false,
      tabBarEnd: sourceTabBarEnd
    }))
  )
  // Select the tab of the file that the panel shows, also when `currentFile` is a fallback.
  $effect(() => {
    if (currentFile) {
      sourceCurrentTab = currentFile.name
    }
  })

  // Analysis panel (Metrics / Connectors / Health / Logs / Config / Issues) tab spec.
  const analysisTabProps = $derived<AnalysisTabProps>({
    metricsMode,
    tooltipData,
    rootNodeId: diagramRootNodeId,
    globalMetrics,
    connectorMetrics,
    connectorStatus,
    runtimeConfig,
    pipelineEvents,
    collectedAt: selectedTimestamp,
    metricLevel,
    lookup,
    logText,
    triageResults,
    issueSeverityFilter,
    issueCategoryFilter,
    onSearchNode: (query) => profilerDiagram?.search(query)
  })
  const analysisTabs = $derived<TabSpec<AnalysisTabProps>[]>([
    {
      id: 'Metrics',
      label: metricsLabel,
      title: 'Show the measurements of the pipeline, of the current node, or of the top nodes',
      panel: MetricsTab,
      keepAlive: true,
      tabBarEnd: metricsTabBarEnd
    },
    {
      id: 'Connectors',
      label: connectorsLabel,
      title: 'Show the statistics and errors of the input and output connectors',
      panel: ConnectorsTab,
      keepAlive: true,
      // The tab stays visible, but is disabled when the bundle has no connector statistics.
      disabled: connectorMetrics === undefined
    },
    {
      id: 'Health',
      label: healthLabel,
      title: 'Show the history of the pipeline status: program, storage and deployment',
      panel: HealthTab,
      keepAlive: true,
      // A pipeline with no monitor history gives an empty list. There is nothing to show then.
      disabled: !pipelineEvents?.length
    },
    {
      id: 'Logs',
      label: logsLabel,
      title: 'Show the pipeline log from the bundle',
      panel: LogsTab,
      keepAlive: true,
      tabBarEnd: commonTabBarEnd
    },
    {
      id: 'Config',
      label: configLabel,
      title: 'Show the pipeline configuration from the bundle',
      panel: ConfigTab,
      keepAlive: true,
      // No runtime config in the bundle → nothing to show, so the tab is present but unselectable.
      disabled: runtimeConfig === undefined
    },
    {
      id: 'Issues',
      label: issuesLabel,
      title: 'Show the problems that the triage rules found in the bundle',
      panel: IssuesTab,
      keepAlive: true,
      tabBarEnd: issuesTabBarEnd
    }
  ])
</script>

<!-- ── Graph panel (dataflow graph) ────────────────────────────────────────── -->
{#snippet graphPanel()}
  <!-- Clip only with a profile: without one the panel is short and clipping would hide the
       "Load profile" dropdown that overflows it. -->
  <div
    class="flex flex-col rounded-container bg-surface-50-950 {hasProfile
      ? 'h-full overflow-hidden'
      : 'overflow-visible'}"
  >
    <!-- Header -->
    <div class="flex flex-shrink-0 flex-wrap items-center gap-2 p-4">
      {@render loadProfileControl?.()}
      <ProfileTimestampSelector {profileFiles} {selectedTimestamp} {onSelectTimestamp} />
      {#if hasProfile}
        <div class="ml-auto">
          <input
            bind:this={nodeSearchInput}
            bind:value={nodeSearchQuery}
            type="text"
            placeholder="Search node"
            title="Search for a node by ID, by table or view name, or by a substring of a persistent ID"
            onkeydown={(e) => {
              if (e.key === 'Enter') {
                handleSearch()
              } else if (e.key === 'Escape') {
                nodeSearchInput?.blur()
              }
            }}
            class="input h-6 w-36 text-sm"
          />
        </div>
      {:else}
        <p class="text-surface-800-200">
          This bundle has no circuit profile, so the dataflow graph is unavailable.
        </p>
      {/if}
    </div>
    <!-- Diagram slot. The actual <ProfilerDiagram> is rendered once outside the layout-
         toggle as a <PersistentContent> overlay so it survives the sqlPanelFullHeight
         layout toggle without re-initialising the Visualizer. The `use:` action mirrors
         this div's bounding rect onto the shared handle. -->
    {#if hasProfile}
      <div use:diagramRect.placeholder class="relative min-h-0 flex-1 bg-white-dark">
        {#if error}
          <div
            class="absolute inset-x-4 top-4 z-10 rounded border border-red-300 bg-white p-3 font-mono text-sm text-red-600 shadow"
          >
            {error}
          </div>
        {/if}
      </div>
    {/if}
  </div>
{/snippet}

<!-- ── Code panel tab-bar-end ──────────────────────────────────────────────── -->
{#snippet sourceTabBarEnd()}
  <button
    class="btn-icon ml-auto text-[20px] hover:preset-tonal-surface fd {sqlPanelFullHeight
      ? 'fd-layout-panel-top'
      : 'fd-layout-panel-left'}"
    title={sqlPanelFullHeight ? 'Collapse code panel' : 'Expand code panel to full height'}
    onclick={() => (sqlPanelFullHeight = !sqlPanelFullHeight)}
    aria-label={sqlPanelFullHeight ? 'Collapse code panel' : 'Expand code panel'}
  ></button>
{/snippet}

<!-- ── Analysis panel labels and tab-bar-end snippets ────────────────────────── -->
{#snippet metricsLabel()}Metrics{/snippet}
{#snippet connectorsLabel()}
  Connectors
  {#if connectorErrorCount > 0}
    <span
      class="ml-1 {statusCounterClass} {statusToneColors.error.chip}"
      title="Total parse, encode and transport errors of all the connectors"
    >
      {connectorErrorCount}
    </span>
  {/if}
{/snippet}
{#snippet healthLabel()}Health{/snippet}
{#snippet logsLabel()}Logs{/snippet}
{#snippet configLabel()}Config{/snippet}
{#snippet issuesLabel()}
  Issues &amp; Suggestions
  {#if triageResults.results.length > 0}
    <span
      class="ml-1 inline-block min-w-5 rounded px-1 font-medium preset-filled-warning-200-800"
      title="Number of issues and suggestions that the triage rules found"
    >
      {triageResults.results.length}
    </span>
  {/if}
{/snippet}

{#snippet commonTabBarEnd()}
  <div class="ml-auto flex min-w-0 flex-wrap items-center justify-end gap-2">
    {#if levelMetricOptions.length > 0}
      <MetricSelect
        bind:value={() => shownMetricId, (id) => (selectedMetricId = id)}
        metrics={levelMetricOptions}
        class="w-42"
        title="Select metric"
      />
    {/if}
    <SearchBar
      bind:this={searchBar}
      bind:value={lookupQuery}
      placeholder={currentTab === 'Logs'
        ? 'Search logs'
        : currentTab === 'Issues'
          ? 'Search issues'
          : 'Search metrics'}
      title="Search within active tab (Enter / Shift+Enter to step matches, Esc to clear, Ctrl/Cmd-F to toggle)"
      results={lookupProgress}
      onnext={() => handleLookup('next')}
      onprevious={() => handleLookup('prev')}
      onclear={resetLookup}
      inputClass="h-8 w-40 text-sm"
    />
  </div>
{/snippet}

{#snippet issuesTabBarEnd()}
  <div class="flex items-center gap-2 px-2">
    <Select
      bind:value={issueSeverityFilter}
      class="select h-6 min-h-0 px-2 py-0! text-sm w-32"
      title="Filter by severity"
    >
      <option class="text-base" value="all">All severity</option>
      {#each issueSeverities as severity (severity)}
        <option class="text-base" value={severity}>{severityLabel[severity] ?? severity}</option>
      {/each}
    </Select>
    <Select
      bind:value={issueCategoryFilter}
      class="select h-6 min-h-0 px-2 py-0! text-sm w-32"
      title="Filter by category"
    >
      <option value="all">Category</option>
      {#each issueCategories as category (category)}
        <option class="text-base" value={category}>{category}</option>
      {/each}
    </Select>
  </div>
  {@render commonTabBarEnd()}
{/snippet}

{#snippet metricsTabBarEnd()}
  <SegmentedControl
    value={metricsMode}
    onValueChange={setMetricsMode}
    items={[
      {
        value: 'overview',
        label: 'Overview',
        title: 'Show the global statistics of the pipeline and the totals of all nodes'
      },
      {
        value: 'node',
        label: 'Node',
        title: 'Show the measurements of the current node',
        disabled: !lastNodeData
      },
      {
        value: 'top-nodes',
        label: 'Top nodes',
        title: 'Show the nodes with the highest values of the current metric',
        disabled: !hasProfile
      }
    ]}
    class="px-2"
  />
  <!-- Also in Top nodes: the level picks the metrics that the metric selector lists. -->
  <SegmentedControl
    value={metricLevel}
    onValueChange={(level) => (metricLevel = level)}
    items={[
      {
        value: 'basic',
        label: 'Basic',
        title: 'Show only the operator runtime, input record count and storage size'
      },
      {
        value: 'standard',
        label: 'Standard',
        title: 'Show the metrics for usual performance analysis'
      },
      { value: 'advanced', label: 'Advanced', title: 'Show all the metrics' }
    ]}
    class="px-2"
  />
  {@render commonTabBarEnd()}
{/snippet}

<!-- ── Code panel (source files via TabsPanel) ───────────────────────────── -->
{#snippet sourcePanel()}
  {#if sourceTabProps}
    <TabsPanel tabs={sourceTabs} bind:currentTab={sourceCurrentTab} tabProps={sourceTabProps} />
  {:else}
    <div
      class="flex h-full items-center justify-center rounded-container bg-surface-50-950 text-surface-600-400"
    >
      Profile does not contain the source code
    </div>
  {/if}
{/snippet}

<!-- ── Analysis panel (tabbed analysis via TabsPanel) ────────────────────────── -->
{#snippet analysisPanel()}
  <TabsPanel
    tabs={analysisTabs}
    bind:currentTab
    tabProps={analysisTabProps}
    tabLabelVariant="segment"
    headerClass="pt-1 pb-2.5"
  />
{/snippet}

<!-- ══ Layout: normal (graph top, SQL + tabs bottom) ══════════════════════ -->
{#if !sqlPanelFullHeight}
  <PaneGroup direction="vertical" class="!overflow-visible h-full">
    {#if hasProfile}
      <Pane
        order={1}
        defaultSize={graphPaneDefaultSize}
        minSize={graphPaneMinSize}
        class="!overflow-visible"
      >
        {@render graphPanel()}
      </Pane>
      <PaneResizer class="pane-divider-horizontal my-2" />
    {:else}
      <div class="mb-4">
        {@render graphPanel()}
      </div>
    {/if}
    <Pane order={2} defaultSize={belowGraphPaneDefaultSize} minSize={15} class="!overflow-visible">
      <PaneGroup direction="horizontal" class="!overflow-visible h-full">
        <Pane defaultSize={50} minSize={0} class="!overflow-visible">
          <div use:sourcePanelRect.placeholder class="h-full"></div>
        </Pane>
        <PaneResizer class="pane-divider-vertical mx-1.5" />
        <Pane minSize={20} class="!overflow-visible">
          {@render analysisPanel()}
        </Pane>
      </PaneGroup>
    </Pane>
  </PaneGroup>
{:else}
  <!-- ══ Layout: SQL full-height (SQL left, graph + tabs right) ═══════════ -->
  <PaneGroup direction="horizontal" class="!overflow-visible h-full">
    <Pane defaultSize={40} minSize={15} class="!overflow-visible">
      <div use:sourcePanelRect.placeholder class="h-full"></div>
    </Pane>
    <PaneResizer class="pane-divider-vertical mx-1.5" />
    <Pane minSize={20} class="!overflow-visible">
      <PaneGroup direction="vertical" class="!overflow-visible h-full">
        {#if hasProfile}
          <Pane
            order={1}
            defaultSize={graphPaneDefaultSize}
            minSize={graphPaneMinSize}
            class="!overflow-visible"
          >
            {@render graphPanel()}
          </Pane>
          <PaneResizer class="pane-divider-horizontal my-2" />
        {:else}
          <div class="mb-4">
            {@render graphPanel()}
          </div>
        {/if}
        <Pane order={2} defaultSize={belowGraphPaneDefaultSize} minSize={15} class="!overflow-visible">
          {@render analysisPanel()}
        </Pane>
      </PaneGroup>
    </Pane>
  </PaneGroup>
{/if}

<!-- Persistent ProfilerDiagram overlay. Always mounted at root, sized to mirror the
     <PersistentPlaceholder> in graphPanel. Surviving the layout toggle (and any other
     conditional re-render of graphPanel) is what keeps the Visualizer's internal state intact
     instead of being torn down and re-created from `profileData` each time. -->
{#if profileData}
  <PersistentContent persistent={diagramRect} class="overflow-hidden bg-white-dark">
    <ProfilerDiagram
      bind:this={profilerDiagram}
      {profileData}
      {dataflowData}
      {programCode}
      {callbacks}
      {theme}
    />
  </PersistentContent>
{/if}

<!-- The progress message ("Computing layout...") next to the pointer, where the user looks. -->
<PointerTooltip
  show={message !== ''}
  class="rounded preset-filled-error-500 px-2 py-1 font-mono text-sm shadow"
>
  {message}
</PointerTooltip>

<!-- Persistent code panel overlay. It renders once and follows the placeholder of the active {#if}
     branch, so the Monaco editor survives the sqlPanelFullHeight toggle. -->
<PersistentContent persistent={sourcePanelRect} class="overflow-visible">
  {@render sourcePanel()}
</PersistentContent>
