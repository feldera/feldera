// Builds the renderable block list for a NodeAttributes payload.
//
// Blocks are grouped by the metric's category, retrieved from the profile metadata
// (`ProfileMetricDescription::category`). Every block renders as a distribution.

import type { NodeAttributes, TooltipRow } from 'profiler-lib'
import { measurementCategory, measurementDescription } from 'profiler-lib'
import { type Format, inferFormat } from './format'

export type RenderableMetric = {
  row: TooltipRow
  label: string
  format: Format
}

export type RenderableBlock = {
  id: string
  title: string
  entries: RenderableMetric[]
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
      label: labelFor(row.metric),
      format: inferFormat(row.cells[0]?.value ?? '')
    })
  }

  const out: RenderableBlock[] = []
  for (const [category, entries] of byCategory) {
    out.push({ id: `category-${slugify(category)}`, title: category, entries })
  }
  return out
}
