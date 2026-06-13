// State machine for the analysis panel's view selection, tracked explicitly between the root and normal nodes.

import type { NodeAttributes } from 'profiler-lib'
import type { MetricsMode } from '../components/MetricsView.svelte'

/** The analysis panel's current selection. `{ node }` carries the operator id so "Node" can be
 *  restored to a specific operator. */
export type AnalysisView = 'overview' | 'top-nodes' | { node: string }

export function metricsModeOf(view: AnalysisView): MetricsMode {
  return typeof view === 'string' ? view : 'node'
}

/** Accepts `null` so "is there a node to restore?" is a single call. */
export function isNodeView(view: AnalysisView | null): view is { node: string } {
  return view !== null && typeof view === 'object'
}

/** First whitespace-delimited token of the title (cytograph builds it as `${id} ${operation}`). */
export function nodeIdOf(attrs: NodeAttributes): string {
  return attrs.title.split(' ')[0] ?? ''
}
