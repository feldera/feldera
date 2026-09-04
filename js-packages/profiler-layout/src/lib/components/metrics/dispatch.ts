// Builds the renderable block list for a NodeAttributes payload.
//
// Blocks are grouped by the metric's category, retrieved from the profile metadata
// (`ProfileMetricDescription::category`). Every block renders as a distribution.

import type { NodeAttributes, TooltipRow } from 'profiler-lib'
import {
  compareMetrics,
  measurementCategory,
  measurementDescription,
  measurementLabel
} from 'profiler-lib'

export type RenderableMetric = {
  row: TooltipRow
  label: string
}

export type RenderableBlock = {
  id: string
  title: string
  entries: RenderableMetric[]
}

const UNCATEGORIZED = 'Other'

const slugify = (s: string): string => s.toLowerCase().replace(/[^a-z0-9]+/g, '-')

export function buildBlocks(attrs: NodeAttributes, showAdvanced: boolean): RenderableBlock[] {
  // Preserve first-seen category order from the rows.
  const byCategory = new Map<string, RenderableMetric[]>()
  for (const row of attrs.rows) {
    if (!showAdvanced && measurementDescription(row.metric).advanced) {
      continue
    }
    const category = measurementCategory(row.metric) || UNCATEGORIZED
    let bucket = byCategory.get(category)
    if (!bucket) {
      bucket = []
      byCategory.set(category, bucket)
    }
    bucket.push({
      row,
      label: measurementLabel(row.metric)
    })
  }

  // Sort metrics inside each block by their displayed label, placing the current metric first.
  // `compareMetrics` is the order the metric selector uses as well, so a metric sits in the same
  // relative position in both.
  for (const entries of byCategory.values()) {
    entries.sort(
      (a, b) =>
        Number(b.row.isCurrentMetric) - Number(a.row.isCurrentMetric) ||
        compareMetrics({ id: a.row.metric, label: a.label }, { id: b.row.metric, label: b.label })
    )
  }

  const out: RenderableBlock[] = []
  for (const [category, entries] of byCategory) {
    out.push({ id: `category-${slugify(category)}`, title: category, entries })
  }
  // Its block leads the panel, so the current metric's value is the first on screen.
  const holdsCurrent = out.findIndex((block) =>
    block.entries.some((entry) => entry.row.isCurrentMetric)
  )
  if (holdsCurrent > 0) {
    out.unshift(...out.splice(holdsCurrent, 1))
  }
  return out
}
