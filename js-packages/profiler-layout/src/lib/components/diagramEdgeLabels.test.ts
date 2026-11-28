// Browser tests for the hover of an edge label. Edges ignore pointer events, so profiler-lib finds the
// label under the pointer itself (`edgeLabels.ts`). This needs a real canvas, because cytoscape
// measures the labels only when it draws them.

import { describe, expect, it } from 'vitest'
import { mountDiagram, settle } from '../test-support/mountDiagram.js'

/** The output statistics of an operator that wrote `records` rows in one batch on one worker. */
const outputRows = (records: number) => [
  {
    metric_id: 'output_batches_stats',
    value: {
      batches_count: { value: 1 },
      min_records_count: { value: records },
      max_records_count: { value: records },
      avg_records_count: { value: records },
      total_records_count: { value: records }
    }
  }
]

/** `src -> map -> sink`, so each edge has a label with its row count. */
const CHAIN = {
  metrics: [],
  worker_profiles: [{ metadata: { src: outputRows(950), map: outputRows(10) } }],
  graph: {
    nodes: {
      id: 'n',
      label: 'circuit',
      nodes: [
        { Simple: { id: 'src', label: 'source' } },
        { Simple: { id: 'map', label: 'map' } },
        { Simple: { id: 'sink', label: 'sink' } }
      ]
    },
    edges: [
      { from_node: 'src', to_node: 'map' },
      { from_node: 'map', to_node: 'sink' }
    ]
  }
}

/** The rendered center of the label of the edge `id`. */
// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
const labelCenter = (cy: any, id: string) => {
  const box = cy.$id(id).renderedBoundingBox({
    includeNodes: false,
    includeEdges: false,
    includeLabels: true,
    includeMainLabels: true,
    includeSourceLabels: false,
    includeTargetLabels: false,
    includeOverlays: false
  })
  return { x: (box.x1 + box.x2) / 2, y: (box.y1 + box.y2) / 2 }
}

describe('edge label hover', () => {
  it('reports the source node of the edge, as if the pointer hovered the node', async () => {
    const { cy, pointer, reported, cleanup } = await mountDiagram('light', CHAIN)
    cy.fit(cy.elements(), 20)
    await settle()
    expect(cy.$id('src->map').data('label')).toBe('950 rows')

    const label = labelCenter(cy, 'src->map')
    pointer('mousemove', label.x, label.y)
    await settle()
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'src', isSticky: false })
    expect(cy.$id('src').hasClass('selected-node')).toBe(true)

    // The top left corner of the view is empty, so the hover of `src` ends there.
    pointer('mousemove', 2, 2)
    await settle()
    expect(reported.attributes.at(-1)).toEqual({ nodeId: null, isSticky: false })
    cleanup()
  })
})
