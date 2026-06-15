import { describe, expect, it } from 'vitest'
import { type AnalysisView, isNodeView, metricsModeOf } from './metricsMode.js'

describe('metricsModeOf', () => {
  it("returns 'overview' and 'top-nodes' unchanged", () => {
    expect(metricsModeOf('overview')).toBe('overview')
    expect(metricsModeOf('top-nodes')).toBe('top-nodes')
  })

  it("maps any `{ node }` to 'node' regardless of the operator id", () => {
    expect(metricsModeOf({ nodeId: '42' })).toBe('node')
    expect(metricsModeOf({ nodeId: 'persistent-id-xyz' })).toBe('node')
  })

  // Regression for the old title-based guess: the mode used to be inferred from
  // `tooltipData.title === 'n region'`, so any profile whose root id wasn't `n` kept showing
  // "Node" by mistake. Reading the mode straight from the `AnalysisView` removes that guesswork.
  it('reads the mode from the `AnalysisView` for any root id', () => {
    const seq: AnalysisView[] = ['overview', { nodeId: 'root-42' }, 'top-nodes', 'overview']
    expect(seq.map(metricsModeOf)).toEqual(['overview', 'node', 'top-nodes', 'overview'])
  })
})

describe('isNodeView', () => {
  it('narrows to `{ node }` and returns false for string variants', () => {
    const v: AnalysisView = { nodeId: 'op-7' }
    if (isNodeView(v)) {
      expect(v.nodeId).toBe('op-7')
    } else {
      throw new Error('expected isNodeView to narrow')
    }
    expect(isNodeView('overview')).toBe(false)
    expect(isNodeView('top-nodes')).toBe(false)
    expect(isNodeView(null)).toBe(false)
  })
})
