<script lang="ts" module>
  import type { TriageResults } from 'triage-types'
  import type { LookupCoordinator } from '../../functions/lookup'
  import type { MetricsMode } from '../MetricsView.svelte'
  import type { TooltipData } from '../ProfilerTooltip.svelte'

  /** Shared bag of props passed to every analysis-panel tab (Metrics / Logs / Issues). Each tab
   *  uses a subset; declared uniformly so `TabsPanel<T>` can type-check with a single T. */
  export type AnalysisTabProps = {
    metricsMode: MetricsMode
    tooltipData: TooltipData | null
    /** Id of the loaded profile's toplevel node, so the Metrics view can tell the overview from a
     *  single operator. `undefined` until a profile is loaded. */
    rootNodeId: string | undefined
    /** When true, metrics flagged `advanced` in the profile metadata are shown too. */
    showAdvancedMetrics: boolean
    lookup: LookupCoordinator
    logText: string | undefined
    triageResults: TriageResults
    /** 'all' or a Severity. Filters Issues tab rows. */
    issueSeverityFilter: 'all' | 'error' | 'warning' | 'info'
    /** 'all' or a category string (case-sensitive, as returned by `getCategory`). */
    issueCategoryFilter: string
    /** Links the metrics node title back to (searches for) the node in the diagram. */
    onSearchNode?: (query: string) => void
    /** Called when the user presses the search shortcut inside the tab. */
    onSearchShortcut?: () => void
  }
</script>

<script lang="ts">
  import MetricsView from '../MetricsView.svelte'
  let {
    metricsMode,
    tooltipData,
    rootNodeId,
    showAdvancedMetrics,
    lookup,
    onSearchNode
  }: AnalysisTabProps = $props()
</script>

<MetricsView
  mode={metricsMode}
  {tooltipData}
  {rootNodeId}
  showAdvanced={showAdvancedMetrics}
  {lookup}
  {onSearchNode}
/>
