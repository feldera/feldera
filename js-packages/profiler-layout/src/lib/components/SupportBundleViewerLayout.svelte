<script lang="ts">
  import { Switch } from '@skeletonlabs/skeleton-svelte'
  import type { ZipItem } from 'but-unzip'
  import {
    PersistentContent,
    SegmentedControl,
    Select,
    type TabSpec,
    TabsPanel,
    usePersistentRect
  } from 'common-ui'
  import { Pane, PaneGroup, PaneResizer } from 'paneforge'
  import type {
    Dataflow,
    JsonProfiles,
    MetricOption,
    NodeAttributes,
    ProfilerCallbacks,
    SourcePositionRange,
    WorkerOption
  } from 'profiler-lib'
  import { type Snippet, untrack } from 'svelte'
  import type { TriageResults } from 'triage-types'
  import { createLookupCoordinator } from '../functions/lookup'
  import { type AnalysisView, isNodeView, metricsModeOf, nodeIdOf } from '../functions/metricsMode'
  import { severityLabel, uniqueCategories, uniqueSeverities } from '../functions/triage'
  import type { MetricsMode } from './MetricsView.svelte'
  import ProfilerDiagram from './ProfilerDiagram.svelte'
  import type { TooltipData } from './ProfilerTooltip.svelte'
  import ProfileTimestampSelector from './ProfileTimestampSelector.svelte'
  import IssuesTab from './tabs/IssuesTab.svelte'
  import LogsTab from './tabs/LogsTab.svelte'
  import MetricsTab, { type AnalysisTabProps } from './tabs/MetricsTab.svelte'
  import SqlTab, { type SqlTabProps } from './tabs/SqlTab.svelte'

  const TABS = ['Metrics', 'Logs', 'Issues'] as const
  type AnalysisTab = (typeof TABS)[number]

  interface Props {
    profileData: JsonProfiles
    dataflowData: Dataflow | undefined
    programCode: string[] | undefined
    logText?: string
    triageResults: TriageResults
    profileFiles: [Date, ZipItem[]][]
    selectedTimestamp: Date | null
    onSelectTimestamp: (timestamp: Date) => void
    sqlPanelFullHeight: boolean
    /** Snippet for the "Load profile" button and timestamp selector in graph-panel header */
    loadProfileControl?: Snippet
    /** Optional slot for a richer SQL panel; receives current highlight ranges */
    sqlPanel?: Snippet<[highlightRanges: SourcePositionRange[]]>
    onHighlightSourceRanges?: (ranges: SourcePositionRange[]) => void
    /** Fired when the graph rendering enters or leaves its asynchronous layout phase.
     */
    onRenderingChange?: (rendering: boolean) => void
  }

  let {
    profileData,
    dataflowData,
    programCode,
    logText,
    triageResults,
    profileFiles,
    selectedTimestamp,
    onSelectTimestamp,
    sqlPanelFullHeight = $bindable(),
    loadProfileControl,
    sqlPanel,
    onHighlightSourceRanges,
    onRenderingChange
  }: Props = $props()

  let profilerDiagram: ProfilerDiagram | undefined = $state()
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
  let showAdvancedMetrics = $state(false)
  let issueSeverityFilter = $state<'all' | 'error' | 'warning' | 'info'>('all')
  let issueCategoryFilter = $state<string>('all')
  const issueCategories = $derived(uniqueCategories(triageResults.results))
  const issueSeverities = $derived(uniqueSeverities(triageResults.results))
  let nodeSearchQuery = $state('')
  let lookupQuery = $state('')
  // Bound to the shared analysis-panel lookup <input>. Tabs (e.g. the log list) can request
  // focus via the `onSearchShortcut` callback threaded through `analysisTabProps`.
  let searchInputEl: HTMLInputElement | undefined = $state()
  const onAnalysisSearchShortcut = () => {
    searchInputEl?.focus()
    searchInputEl?.select()
  }
  let highlightRanges: SourcePositionRange[] = $state([])

  // The graph panel's diagram is hoisted to a <PersistentContent> overlay so it survives the
  // sqlPanelFullHeight layout toggle (which would otherwise unmount/remount the snippet's
  // <ProfilerDiagram>, discarding the Visualizer's state). The graphPanel snippet renders a
  // <PersistentPlaceholder> that mirrors its bounding rect onto this shared handle.
  const diagramRect = usePersistentRect()

  // The whole SQL panel is hoisted the same way. The layout toggle swaps which {#if} branch
  // renders the panel, and Svelte mounts the new branch before tearing down the old — so an
  // inline SQL panel would briefly have two Monaco editors fighting over one model URI on every
  // expand/collapse. Hoisting keeps a single editor instance (and a single, simply-disposed
  // model) alive across the toggle; each layout branch only renders a placeholder.
  const sqlPanelRect = usePersistentRect()

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
          currentTab = 'Metrics'
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
      analysisView = { node: nodeId }
      currentTab = 'Metrics'
    },
    displayTopNodes(data, _isSticky) {
      tooltipData = data.match({
        some: (topNodes) => ({
          genericTable: {
            header: `Nodes with highest values for "${selectedMetricId}"`,
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
    onNodeDoubleClick: (nodeId, type) => {
      if (type !== 'leaf') {
        return
      }
      const profile = profilerDiagram?.getProfile()
      if (!profile) {
        return
      }
      const ranges = profile.getSourceRanges(nodeId)
      if (ranges.length === 0) {
        return
      }
      highlightRanges = ranges
      onHighlightSourceRanges?.(ranges)
    },
    onRenderingChange: (rendering) => {
      onRenderingChange?.(rendering)
    }
  }

  // Re-color the diagram when the selected metric changes. The selectMetric() call refreshes
  // the current sticky tooltip via the callbacks above, but those only touch tooltipData — so
  // this does not feed back into any effect (no loop). Same shape as the original ProfilerLayout.
  $effect(() => {
    profilerDiagram?.selectMetric(selectedMetricId)
  })

  // Show the overview each time a profile loads.
  $effect(() => {
    void profileData
    const diagram = profilerDiagram
    if (!diagram) {
      return
    }
    queueMicrotask(() => diagram.showGlobalMetrics(true))
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
      analysisView = { node: nodeIdOf(lastNodeData) }
      tooltipData = { nodeAttributes: lastNodeData }
    }
  }

  function handleSearch() {
    if (nodeSearchQuery) {
      profilerDiagram?.search(nodeSearchQuery)
    }
  }

  function handleLookup() {
    if (lookupQuery) {
      lookup.execute(currentTab, lookupQuery)
    }
  }

  $effect(() => {
    currentTab
    lookupQuery = ''
  })

  // SQL panel tab state. Single-tab; bound state is required by TabsPanel.
  let sqlCurrentTab = $state('SQL')
  const sqlTabProps = $derived<SqlTabProps>({
    sqlPanel,
    code: programCode?.join('\n') ?? '',
    highlightRanges
  })
  const sqlTabs = $derived<TabSpec<SqlTabProps>[]>([
    { id: 'SQL', label: sqlLabel, panel: SqlTab, keepAlive: false, tabBarEnd: sqlTabBarEnd }
  ])

  // Analysis panel (Metrics / Logs / Issues) tab spec.
  const analysisTabProps = $derived<AnalysisTabProps>({
    metricsMode,
    tooltipData,
    showAdvancedMetrics,
    lookup,
    logText,
    triageResults,
    issueSeverityFilter,
    issueCategoryFilter,
    onSearchNode: (query) => profilerDiagram?.search(query),
    onSearchShortcut: onAnalysisSearchShortcut
  })
  const analysisTabs = $derived<TabSpec<AnalysisTabProps>[]>([
    {
      id: 'Metrics',
      label: metricsLabel,
      panel: MetricsTab,
      keepAlive: true,
      tabBarEnd: metricsTabBarEnd
    },
    {
      id: 'Logs',
      label: logsLabel,
      panel: LogsTab,
      keepAlive: true,
      tabBarEnd: commonTabBarEnd
    },
    {
      id: 'Issues',
      label: issuesLabel,
      panel: IssuesTab,
      keepAlive: true,
      tabBarEnd: issuesTabBarEnd
    }
  ])
</script>

<!-- ── Graph panel (dataflow graph) ────────────────────────────────────────── -->
{#snippet graphPanel()}
  <div class="flex h-full flex-col overflow-hidden rounded-container bg-surface-50-950">
    <!-- Header -->
    <div class="flex flex-shrink-0 flex-wrap items-center gap-2 p-4">
      {@render loadProfileControl?.()}
      <ProfileTimestampSelector {profileFiles} {selectedTimestamp} {onSelectTimestamp} />
      <div class="ml-auto">
        <input
          bind:value={nodeSearchQuery}
          type="text"
          placeholder="Search node"
          title="Search for a node by ID or persistent ID"
          onkeydown={(e) => e.key === 'Enter' && handleSearch()}
          class="input h-6 w-36 text-sm"
        />
      </div>
    </div>
    <!-- Diagram slot. The actual <ProfilerDiagram> is rendered once outside the layout-
         toggle as a <PersistentContent> overlay so it survives the sqlPanelFullHeight
         layout toggle without re-initialising the Visualizer. The `use:` action mirrors
         this div's bounding rect onto the shared handle. -->
    <div use:diagramRect.placeholder class="relative min-h-0 flex-1 bg-white-dark">
      {#if error}
        <div
          class="absolute inset-x-4 top-4 z-10 rounded border border-red-300 bg-white p-3 font-mono text-sm text-red-600 shadow"
        >
          {error}
        </div>
      {/if}
      {#if message}
        <div
          class="absolute left-2 top-2 z-10 rounded bg-white/90 px-2 py-1 font-mono text-sm shadow"
        >
          {message}
        </div>
      {/if}
    </div>
  </div>
{/snippet}

<!-- ── SQL panel label, tab-bar-end, and tab spec ───────────────────────── -->
{#snippet sqlLabel()}
  <span class="">program.sql</span>
{/snippet}

{#snippet sqlTabBarEnd()}
  <button
    class="btn-icon ml-auto text-[20px] hover:preset-tonal-surface fd {sqlPanelFullHeight
      ? 'fd-layout-panel-top'
      : 'fd-layout-panel-left'}"
    title={sqlPanelFullHeight ? 'Collapse SQL panel' : 'Expand SQL panel to full height'}
    onclick={() => (sqlPanelFullHeight = !sqlPanelFullHeight)}
    aria-label={sqlPanelFullHeight ? 'Collapse SQL panel' : 'Expand SQL panel'}
  ></button>
{/snippet}

<!-- ── Analysis panel labels and tab-bar-end snippets ────────────────────────── -->
{#snippet metricsLabel()}Metrics{/snippet}
{#snippet logsLabel()}Logs{/snippet}
{#snippet issuesLabel()}
  Issues &amp; Suggestions
  {#if triageResults.results.length > 0}
    <span class="ml-1 inline-block min-w-5 rounded px-1 font-medium preset-filled-warning-200-800">
      {triageResults.results.length}
    </span>
  {/if}
{/snippet}

{#snippet commonTabBarEnd()}
  <div class="ml-auto flex min-w-0 flex-wrap items-center justify-end gap-2">
    {#if metrics.length > 0}
      <Select
        bind:value={selectedMetricId}
        class="px-2 w-42"
        title="Select metric"
      >
        {#each metrics as metric (metric.id)}
          <option class="text-base" value={metric.id}>{metric.label}</option>
        {/each}
      </Select>
    {/if}
    <input
      bind:this={searchInputEl}
      bind:value={lookupQuery}
      type="text"
      placeholder={currentTab === 'Logs'
        ? 'Search logs'
        : currentTab === 'Issues'
          ? 'Search issues'
          : 'Search metrics'}
      title="Search within active tab (Enter to jump, Ctrl/Cmd-F to focus)"
      onkeydown={(e) => e.key === 'Enter' && handleLookup()}
      class="input h-6 w-28 text-sm"
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
      { value: 'overview', label: 'Overview' },
      { value: 'node', label: 'Node', disabled: !lastNodeData },
      { value: 'top-nodes', label: 'Top nodes' }
    ]}
    class="px-2"
  />
  {#if metricsMode !== 'top-nodes'}
    <label class="flex h-6 cursor-pointer items-center gap-2 text-sm">
      <Switch
        name="showAdvancedMetrics"
        checked={showAdvancedMetrics}
        onCheckedChange={(e) => (showAdvancedMetrics = e.checked)}
      >
        <Switch.Control class="h-5 w-9">
          <Switch.Thumb />
        </Switch.Control>
        <Switch.HiddenInput />
      </Switch>
      Show advanced
    </label>
  {/if}
  {@render commonTabBarEnd()}
{/snippet}

<!-- ── SQL panel (SQL code view via TabsPanel) ──────────────────────────── -->
{#snippet sqlCodePanel()}
  <TabsPanel
    tabs={sqlTabs}
    bind:currentTab={sqlCurrentTab}
    tabProps={sqlTabProps}
  />
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
    <Pane defaultSize={55} minSize={20} class="!overflow-visible">
      {@render graphPanel()}
    </Pane>
    <PaneResizer class="pane-divider-horizontal my-2" />
    <Pane defaultSize={45} minSize={15} class="!overflow-visible">
      <PaneGroup direction="horizontal" class="!overflow-visible h-full">
        <Pane defaultSize={50} minSize={0} class="!overflow-visible">
          <div use:sqlPanelRect.placeholder class="h-full"></div>
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
      <div use:sqlPanelRect.placeholder class="h-full"></div>
    </Pane>
    <PaneResizer class="pane-divider-vertical mx-1.5" />
    <Pane minSize={20} class="!overflow-visible">
      <PaneGroup direction="vertical" class="!overflow-visible h-full">
        <Pane defaultSize={55} minSize={20} class="!overflow-visible">
          {@render graphPanel()}
        </Pane>
        <PaneResizer class="pane-divider-horizontal my-2" />
        <Pane defaultSize={45} minSize={15} class="!overflow-visible">
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
<PersistentContent persistent={diagramRect} class="overflow-hidden bg-white-dark">
  <ProfilerDiagram
    bind:this={profilerDiagram}
    {profileData}
    {dataflowData}
    {programCode}
    {callbacks}
  />
</PersistentContent>

<!-- Persistent SQL panel overlay. Hoisted out of the layout branches for the same reason as the
     diagram: rendered once, it mirrors the placeholder in whichever {#if} branch is active, so the
     Monaco editor and its model are created once and survive the sqlPanelFullHeight toggle. -->
<PersistentContent persistent={sqlPanelRect} class="overflow-visible">
  {@render sqlCodePanel()}
</PersistentContent>
