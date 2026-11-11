// Browser tests for the node report and the node shading after a change of the metric or of the
// visible workers. The report that a click pinned must follow each change.

import { describe, expect, it } from 'vitest'
import { mountDiagram, settle } from '../test-support/mountDiagram.js'

const bytes = (value: number) => ({ type: 'bytes', value })
const count = (value: number) => ({ type: 'count', value })

/** Two operators with two metrics, on two workers. */
const TWO_WORKERS = {
  metrics: [],
  worker_profiles: [0, 1].map((worker) => ({
    metadata: {
      n1: [
        { metric_id: 'used_bytes', value: bytes(1024 * (worker + 1)) },
        { metric_id: 'input_records_count', value: count(10 * (worker + 1)) }
      ],
      n2: [
        { metric_id: 'used_bytes', value: bytes(4096 * (worker + 1)) },
        { metric_id: 'input_records_count', value: count(5 * (worker + 1)) }
      ]
    }
  })),
  graph: {
    nodes: {
      id: 'n',
      label: 'circuit',
      nodes: [{ Simple: { id: 'n1', label: 'map' } }, { Simple: { id: 'n2', label: 'filter' } }]
    },
    edges: []
  }
}

describe('a change of the metric or of the visible workers', () => {
  it('updates the pinned node report and the shading', async () => {
    const { cy, diagram, reported, cleanup } = await mountDiagram('light', TWO_WORKERS)
    cy.$id('n1').emit('click')
    await settle()
    expect(reported.lastReport.columns).toHaveLength(2)

    diagram.selectMetric('input_records_count')
    await settle()
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'n1', isSticky: true })
    expect(reported.lastReport.currentMetric).toBe('input_records_count')

    diagram.selectMetric('used_bytes')
    await settle()
    expect(reported.lastReport.currentMetric).toBe('used_bytes')

    // The shade of `n1` sits between the readings of the visible workers, so it changes when the
    // smallest reading is hidden.
    const shade = cy.$id('n1').data('value')
    diagram.toggleWorker('0')
    await settle()
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'n1', isSticky: true })
    expect(reported.lastReport.columns).toEqual(['1'])
    expect(cy.$id('n1').data('value')).not.toBe(shade)
    cleanup()
  })
})
