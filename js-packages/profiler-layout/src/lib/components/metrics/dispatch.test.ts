import {
  MetricDescriptions,
  type MetricLevel,
  type NodeAttributes,
  type TooltipRow
} from 'profiler-lib'
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
    const blocks = buildBlocks(attrs, 'advanced')
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
    const blocks = buildBlocks(attrs, 'advanced')
    const labels = blocks.flatMap((b) => b.entries.map((e) => e.label))
    expect(labels).toEqual(['Slot 1 loose', 'Slot 2 loose', 'Slot 10 loose'])
  })
})

describe('buildBlocks detail levels', () => {
  const metrics = [
    'runtime_seconds',
    'steps_count',
    'input_batches_stats.count',
    'input_batches_stats.record_count',
    'spine_storage_size_bytes'
  ]
  const shown = (level: MetricLevel) =>
    buildBlocks(makeAttrs(metrics), level).flatMap((b) => b.entries.map((e) => e.row.metric))

  it('shows only runtime, input records and storage at the basic level', () => {
    expect(shown('basic').sort()).toEqual([
      'input_batches_stats.record_count',
      'runtime_seconds',
      'spine_storage_size_bytes'
    ])
  })

  it('shows the basic metrics and the rest at the standard level', () => {
    expect(shown('standard').sort()).toEqual([...metrics].sort())
  })

  it('puts the basic metrics in one block, whatever their categories', () => {
    const metric = (name: string, category: string) => ({
      name,
      category,
      advanced: false,
      description: ''
    })
    MetricDescriptions.setMetricDescriptions([
      metric('runtime_seconds', 'Time'),
      metric('steps_count', 'Time'),
      metric('input_batches_stats', 'Inputs'),
      metric('spine_storage_size_bytes', 'State')
    ])
    const titles = (level: MetricLevel) =>
      buildBlocks(makeAttrs(metrics), level).map((b) => b.title)
    expect(titles('basic')).toEqual(['Basic metrics'])
    expect(titles('standard')).toEqual(['Time', 'Inputs', 'State'])
  })
})

// Issue 6990: the metric the diagram is colored by was buried in an alphabetical list
describe('buildBlocks puts the selected metric first', () => {
  it('leads its block, ahead of labels that sort before it', () => {
    const attrs = makeAttrs(['alpha_count', 'beta_count', 'zeta_count'], 'zeta_count')
    const [block] = buildBlocks(attrs, 'advanced')
    expect(block!.entries.map((e) => e.row.metric)).toEqual([
      'zeta_count',
      'alpha_count',
      'beta_count'
    ])
  })

  it('leads the panel, whichever category it belongs to', () => {
    // 'allocated_memory_bytes' is a State metric and 'runtime_seconds' a Time one, so they land
    // in different blocks; the selected one decides which block comes first.
    const metric = (name: string, category: string) => ({
      name,
      category,
      advanced: false,
      description: ''
    })
    MetricDescriptions.setMetricDescriptions([
      metric('allocated_memory_bytes', 'State'),
      metric('runtime_seconds', 'Time')
    ])
    const metrics = ['allocated_memory_bytes', 'runtime_seconds']
    const first = (current: string) => buildBlocks(makeAttrs(metrics, current), 'advanced')[0]!
    expect(first('runtime_seconds').entries[0]!.row.metric).toBe('runtime_seconds')
    expect(first('allocated_memory_bytes').entries[0]!.row.metric).toBe('allocated_memory_bytes')
    // Two blocks either way: the selected metric moves to the front, stays in its category.
    expect(buildBlocks(makeAttrs(metrics, 'runtime_seconds'), 'advanced')).toHaveLength(2)
  })

  it('leaves the order alone when no metric is selected', () => {
    const attrs = makeAttrs(['alpha_count', 'beta_count'])
    const [block] = buildBlocks(attrs, 'advanced')
    expect(block!.entries.map((e) => e.row.metric)).toEqual(['alpha_count', 'beta_count'])
  })
})
