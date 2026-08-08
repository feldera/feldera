import { describe, expect, it } from 'vitest'
import {
  buildSearchTargets,
  type GlobalEntry,
  type MetricBlock,
  matchTargets,
  type TopNodeRow
} from './metricsSearch'

const globalEntries: GlobalEntry[] = [
  { key: 'total_input_records', label: 'Input records' },
  { key: 'total_input_bytes', label: 'Input bytes' }
]

const blocks: MetricBlock[] = [
  {
    id: 'block-throughput',
    title: 'Throughput',
    entries: [{ label: 'Records/s', row: { metric: 'records_per_sec' } }]
  }
]

const topNodeRows: TopNodeRow[] = [
  { stub: { text: 'JoinNode' }, cells: [{ operation: 'join' }] },
  { stub: { text: 'FilterNode' }, cells: [{ operation: 'filter' }] }
]

describe('buildSearchTargets', () => {
  it('includes the Global stats tile before node blocks in the overview', () => {
    const targets = buildSearchTargets({
      showAttributesView: true,
      globalEntries,
      blocks,
      topNodeRows: []
    })
    expect(targets.map((t) => t.id)).toEqual(['global-metrics', 'block-throughput'])
    expect(targets[0]).toMatchObject({
      title: 'Global stats',
      labels: ['Input records', 'Input bytes']
    })
  })

  it('omits the Global stats tile when there are no global entries (e.g. single-node view)', () => {
    const targets = buildSearchTargets({
      showAttributesView: true,
      globalEntries: [],
      blocks,
      topNodeRows: []
    })
    expect(targets.map((t) => t.id)).toEqual(['block-throughput'])
  })

  it('builds one target per row for the top-nodes view, anchored by row index', () => {
    const targets = buildSearchTargets({
      showAttributesView: false,
      globalEntries,
      blocks,
      topNodeRows
    })
    expect(targets).toEqual([
      { id: 'top-node-0', title: 'JoinNode', labels: ['join'], keys: [] },
      { id: 'top-node-1', title: 'FilterNode', labels: ['filter'], keys: [] }
    ])
  })
})

describe('matchTargets', () => {
  const overview = buildSearchTargets({
    showAttributesView: true,
    globalEntries,
    blocks,
    topNodeRows: []
  })
  const topNodes = buildSearchTargets({
    showAttributesView: false,
    globalEntries,
    blocks,
    topNodeRows
  })

  it('matches the Global stats tile by its entry label', () => {
    expect(matchTargets(overview, 'input records')).toEqual(['global-metrics'])
  })

  it('matches the Global stats tile by its title', () => {
    expect(matchTargets(overview, 'global')).toEqual(['global-metrics'])
  })

  it('matches a top-nodes row by node name and by operation', () => {
    expect(matchTargets(topNodes, 'filter')).toEqual(['top-node-1'])
    expect(matchTargets(topNodes, 'join')).toEqual(['top-node-0'])
  })

  it('ranks title matches ahead of label/key matches, deduped', () => {
    // "records" hits the Global stats entry label and the block's label + metric key. The block
    // title does not contain it, so global-metrics (label tier) comes before block-throughput.
    expect(matchTargets(overview, 'records')).toEqual(['global-metrics', 'block-throughput'])
  })

  it('returns no matches for an empty query or when nothing matches', () => {
    expect(matchTargets(overview, '   ')).toEqual([])
    expect(matchTargets(topNodes, 'nonexistent')).toEqual([])
  })
})
