// Describes which view the analysis panel is showing, so the segmented control stays in sync
// with what the diagram displays.

import type { MetricsMode } from '../components/MetricsView.svelte'

/** Which view the analysis panel is currently showing: the whole-circuit overview, the ranked
 *  top nodes, or a single operator's metrics. When one operator is selected, its id is stored so the
 *  panel can return to that same operator later. */
export type AnalysisView = 'overview' | 'top-nodes' | { node: string }

export function metricsModeOf(view: AnalysisView): MetricsMode {
  return typeof view === 'string' ? view : 'node'
}

/** Accepts `null` so "is there a node to restore?" is a single call. */
export function isNodeView(view: AnalysisView | null): view is { node: string } {
  return view !== null && typeof view === 'object'
}
