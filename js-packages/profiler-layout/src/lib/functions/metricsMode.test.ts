import { describe, expect, it } from 'vitest'
import { type AnalysisView, isNodeView, metricsModeOf } from './metricsMode.js'

describe('metricsModeOf', () => {
  it("projects 'overview' and 'top-nodes' identically", () => {
    expect(metricsModeOf('overview')).toBe('overview')
    expect(metricsModeOf('top-nodes')).toBe('top-nodes')
  })

  it("projects any `{ node }` to 'node' regardless of the operator id", () => {
    expect(metricsModeOf({ node: '42' })).toBe('node')
    expect(metricsModeOf({ node: 'persistent-id-xyz' })).toBe('node')
  })

  // Regression for the title-heuristic bug: previously the mode was derived from
  // `tooltipData.title === 'n region'`, so any profile whose root id wasn't `n` got stuck on
  // "Node". With the explicit discriminator the projection is heuristic-free.
  it('follows the explicit state for any root id', () => {
    const seq: AnalysisView[] = ['overview', { node: 'root-42' }, 'top-nodes', 'overview']
    expect(seq.map(metricsModeOf)).toEqual(['overview', 'node', 'top-nodes', 'overview'])
  })
})

describe('isNodeView', () => {
  it('narrows to `{ node }` and returns false for string variants', () => {
    const v: AnalysisView = { node: 'op-7' }
    if (isNodeView(v)) {
      expect(v.node).toBe('op-7')
    } else {
      throw new Error('expected isNodeView to narrow')
    }
    expect(isNodeView('overview')).toBe(false)
    expect(isNodeView('top-nodes')).toBe(false)
    expect(isNodeView(null)).toBe(false)
  })
})
