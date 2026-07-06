// Builds the renderable block list for a NodeAttributes payload.
//
// Blocks are grouped by the metric's category (from `ProfileMetricDescription::category`).
// Within a block, the sub-rows a composite metric was split into by `parseValues` (e.g.
// `output_batches_stats.count`, `.avg_size`, …) are regrouped under their shared base id into
// a single `MetricGroup`, tagged with the visualization kind. See ./README.md.

import type { NodeAttributes, PropertyValue } from 'profiler-lib'
import { measurementCategory, measurementDescription } from 'profiler-lib'
import { type MetricKind, metricKind, splitBaseId } from './kind'

/** One decoded (sub)field of a metric, carrying its per-worker values (indexed by worker). */
export type MetricSubRow = {
  /** Suffix after the base id: '' for scalars, e.g. '.count', '.used', '.slot:0.steps'. */
  suffix: string
  /** Humanized label of the full metric id (base + suffix). */
  label: string
  values: PropertyValue[]
  /** Profile-wide value range for this metric (min/max across all nodes and workers). */
  range?: { min: number; max: number }
}

/** A metric and all its sub-rows, grouped for rendering by a single kind renderer. */
export type MetricGroup = {
  /** Base metric id (before the first '.'), e.g. 'output_batches_stats'. */
  baseId: string
  /** Humanized label of the base id. */
  label: string
  kind: MetricKind
  /** Sub-rows sharing this base id, in first-seen order. Scalars have exactly one, with ''. */
  rows: MetricSubRow[]
}

export type RenderableBlock = {
  id: string
  title: string
  entries: MetricGroup[]
}

const UNCATEGORIZED = 'Other'

const labelFor = (id: string): string => {
  // Humanize id: replace separators with spaces, capitalize first letter.
  const cleaned = id.replace(/[_.]/g, ' ').replace(/\s+/g, ' ').trim()
  if (cleaned.length === 0) {
    return id
  }
  return cleaned.charAt(0).toUpperCase() + cleaned.slice(1)
}

const slugify = (s: string): string => s.toLowerCase().replace(/[^a-z0-9]+/g, '-')

export function buildBlocks(attrs: NodeAttributes, showAdvanced: boolean): RenderableBlock[] {
  // Preserve first-seen category order; within a category, group rows by base id.
  const byCategory = new Map<string, Map<string, MetricGroup>>()
  for (const row of attrs.rows) {
    if (!showAdvanced && measurementDescription(row.metric).advanced) {
      continue
    }
    const category = measurementCategory(row.metric) || UNCATEGORIZED
    const { baseId, suffix } = splitBaseId(row.metric)
    let groups = byCategory.get(category)
    if (!groups) {
      groups = new Map()
      byCategory.set(category, groups)
    }
    let group = groups.get(baseId)
    if (!group) {
      group = { baseId, label: labelFor(baseId), kind: metricKind(baseId), rows: [] }
      groups.set(baseId, group)
    }
    group.rows.push({
      suffix,
      label: labelFor(row.metric),
      values: row.cells.map((c) => c.value),
      ...(row.range ? { range: row.range } : {})
    })
  }

  // Sort groups inside each block by their displayed label so users can scan a long block
  // without re-reading the whole thing. Locale-aware compare with `numeric: true`
  // keeps numbered labels like "slot 2 ..." / "slot 10 ..." in their natural sequence.
  const collator = new Intl.Collator(undefined, { sensitivity: 'base', numeric: true })
  const out: RenderableBlock[] = []
  for (const [category, groups] of byCategory) {
    const entries = [...groups.values()].sort(
      (a, b) => collator.compare(a.label, b.label) || collator.compare(a.baseId, b.baseId)
    )
    out.push({ id: `category-${slugify(category)}`, title: category, entries })
  }
  return out
}
