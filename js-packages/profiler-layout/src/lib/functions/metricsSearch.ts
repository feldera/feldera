// Search over the Metrics tab's three views (overview "Global stats" tile, per-node metric blocks,
// and the "Top nodes" table). The view builds a flat list of scroll targets in DOM order; the
// matcher ranks them so the panel's search input can cycle through matches.

/** A scrollable match target. `id` is the element's `data-block-id`; the three text tiers are
 *  ranked title first, then secondary labels, then keys. */
export type SearchTarget = { id: string; title?: string; labels: string[]; keys: string[] }

/** Minimal structural shapes taken from the component's derived data (kept loose on purpose so
 *  this module does not depend on profiler-lib / tooltip types). */
export type MetricBlock = {
  id: string
  title?: string
  entries: { label: string; row: { metric: string } }[]
}
export type GlobalEntry = { key: string; label: string }
export type TopNodeRow = { stub: { text: string }; cells: { operation: string }[] }

/**
 * Build the ordered list of search targets for the currently shown Metrics view.
 *
 * Attributes view (overview / single node): the "Global stats" tile first (overview only, when it
 * has entries), then each node metric block. Top-nodes view: one target per table row.
 */
export function buildSearchTargets(view: {
  showAttributesView: boolean
  globalEntries: GlobalEntry[]
  blocks: MetricBlock[]
  topNodeRows: TopNodeRow[]
}): SearchTarget[] {
  if (view.showAttributesView) {
    const targets: SearchTarget[] = []
    if (view.globalEntries.length > 0) {
      targets.push({
        id: 'global-metrics',
        title: 'Global stats',
        labels: view.globalEntries.map((e) => e.label),
        keys: view.globalEntries.map((e) => e.key)
      })
    }
    for (const b of view.blocks) {
      targets.push({
        id: b.id,
        title: b.title,
        labels: b.entries.map((e) => e.label),
        keys: b.entries.map((e) => e.row.metric)
      })
    }
    return targets
  }
  return view.topNodeRows.map((row, i) => ({
    id: `top-node-${i}`,
    title: row.stub.text,
    labels: row.cells.map((c) => c.operation),
    keys: []
  }))
}

/**
 * Return every target's id whose text contains `query` (case-insensitive), most-relevant first and
 * deduped: all title matches, then label matches, then key matches. Empty query → no matches.
 */
export function matchTargets(targets: SearchTarget[], query: string): string[] {
  const q = query.trim().toLowerCase()
  if (!q || targets.length === 0) {
    return []
  }
  const ids: string[] = []
  const add = (id: string) => {
    if (!ids.includes(id)) {
      ids.push(id)
    }
  }
  for (const t of targets) {
    if (t.title?.toLowerCase().includes(q)) {
      add(t.id)
    }
  }
  for (const t of targets) {
    if (t.labels.some((l) => l.toLowerCase().includes(q))) {
      add(t.id)
    }
  }
  for (const t of targets) {
    if (t.keys.some((k) => k.toLowerCase().includes(q))) {
      add(t.id)
    }
  }
  return ids
}
