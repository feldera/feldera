import { describe, expect, it } from 'vitest'
import type { NodeAttributes, TooltipRow } from 'profiler-lib'
import { buildBlocks } from './dispatch.js'

function row(metric: string): TooltipRow {
  return { metric, isCurrentMetric: false, cells: [] }
}

// Minimal NodeAttributes carrying only a list of metric rows; all other fields
// are empty since these tests exercise just the ordering of metrics.
function makeAttrs(metrics: string[]): NodeAttributes {
  return {
    title: 'n op',
    columns: [],
    rows: metrics.map(row),
    attributes: new Map()
  }
}

describe('buildBlocks', () => {
  it('orders metrics lexicographically inside each block (case-insensitive)', () => {
    // Metric IDs span categories; the order is intentionally scrambled to confirm the sort.
    const attrs = makeAttrs([
      'zeta_seconds',
      'Alpha_count',
      'gamma_size',
      'alpha_total'
    ])
    const blocks = buildBlocks(attrs, /* showAdvanced */ true)
    // All metrics fall into the same "Other" bucket (no descriptions registered),
    // so we expect a single block whose entries are alphabetised by label.
    const all = blocks.flatMap((b) => b.entries.map((e) => e.label))
    const sorted = [...all].sort((a, b) =>
      new Intl.Collator(undefined, { sensitivity: 'base', numeric: true }).compare(a, b)
    )
    expect(all).toEqual(sorted)
    // Sanity: the original input was not already sorted.
    expect(attrs.rows.map((r) => r.metric)).not.toEqual(sorted)
  })

  it('uses natural-number ordering, not raw codepoint order', () => {
    // Labels like "slot 2" / "slot 10" share a numeric suffix; a plain ASCII sort would put
    // "slot 10" before "slot 2".
    const attrs = makeAttrs(['slot_10_loose', 'slot_2_loose', 'slot_1_loose'])
    const blocks = buildBlocks(attrs, true)
    const labels = blocks.flatMap((b) => b.entries.map((e) => e.label))
    expect(labels).toEqual(['Slot 1 loose', 'Slot 2 loose', 'Slot 10 loose'])
  })
})
