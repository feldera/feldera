import type { NodeAttributes, TooltipRow } from 'profiler-lib'
import { describe, expect, it } from 'vitest'
import { buildBlocks } from './dispatch.js'

function row(metric: string, isCurrentMetric = false): TooltipRow {
  return { metric, isCurrentMetric, cells: [] }
}

// Minimal NodeAttributes carrying only a list of metric rows; all other fields
// are empty since these tests exercise just the ordering of metrics.
function makeAttrs(metrics: string[], current?: string): NodeAttributes {
  return {
    title: 'n op',
    isRegion: false,
    nodeId: 'n',
    columns: [],
    rows: metrics.map((metric) => row(metric, metric === current)),
    attributes: new Map()
  }
}

describe('buildBlocks', () => {
  it('orders metrics lexicographically inside each block (case-insensitive)', () => {
    // Metric IDs span categories; the order is intentionally scrambled to confirm the sort.
    const attrs = makeAttrs(['zeta_seconds', 'Alpha_count', 'gamma_size', 'alpha_total'])
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

// Issue 6990: the metric the diagram is colored by was buried in an alphabetical list
describe('buildBlocks puts the selected metric first', () => {
  it('leads its block, ahead of labels that sort before it', () => {
    const attrs = makeAttrs(['alpha_count', 'beta_count', 'zeta_count'], 'zeta_count')
    const [block] = buildBlocks(attrs, true)
    expect(block!.entries.map((e) => e.row.metric)).toEqual([
      'zeta_count',
      'alpha_count',
      'beta_count'
    ])
  })

  it('leads the panel, whichever category it belongs to', () => {
    // 'total size' is a memory metric and 'time' a CPU one, so they land in different blocks;
    // the selected one decides which block comes first.
    const metrics = ['total size', 'time']
    const first = (current: string) => buildBlocks(makeAttrs(metrics, current), true)[0]!
    expect(first('time').entries[0]!.row.metric).toBe('time')
    expect(first('total size').entries[0]!.row.metric).toBe('total size')
    // Two blocks either way: the selected metric moves to the front, stays in its category.
    expect(buildBlocks(makeAttrs(metrics, 'time'), true)).toHaveLength(2)
  })

  it('leaves the order alone when no metric is selected', () => {
    const attrs = makeAttrs(['alpha_count', 'beta_count'])
    const [block] = buildBlocks(attrs, true)
    expect(block!.entries.map((e) => e.row.metric)).toEqual(['alpha_count', 'beta_count'])
  })
})
