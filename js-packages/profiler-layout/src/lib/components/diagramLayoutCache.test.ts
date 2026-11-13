// Browser tests for the layout cache of profiler-lib (`layoutCache.ts`). A new diagram of a graph that
// was already laid out, for example after a switch to another snapshot, gets the positions from the
// cache instead of from ELK.

import { describe, expect, it } from 'vitest'
import { mountDiagram } from '../test-support/mountDiagram.js'

const simple = (id: string, label: string) => ({ Simple: { id, label } })

/** A graph that no other test lays out, so the first layout in this file cannot be a cache hit. */
const DIAMOND = {
  metrics: [],
  worker_profiles: [{ metadata: {} }],
  graph: {
    nodes: {
      id: 'n',
      label: 'circuit',
      nodes: [
        simple('n1', 'source'),
        simple('n2', 'left'),
        simple('n3', 'right'),
        simple('n4', 'sink')
      ]
    },
    edges: [
      { from_node: 'n1', to_node: 'n2' },
      { from_node: 'n1', to_node: 'n3' },
      { from_node: 'n2', to_node: 'n4' },
      { from_node: 'n3', to_node: 'n4' }
    ]
  }
}

// biome-ignore lint/suspicious/noExplicitAny: the harness returns an untyped cytoscape instance
const positions = (cy: any) => cy.nodes().map((node: any) => [node.id(), node.position()])

/** True if ELK laid out the diagram. `cytoscape-elk` keeps its input for ELK in the scratch data of
 *  each node, and the preset layout of a cache hit does not. */
// biome-ignore lint/suspicious/noExplicitAny: the harness returns an untyped cytoscape instance
const laidOutByElk = (cy: any) => cy.nodes()[0].scratch('elk') !== undefined

describe('the layout cache', () => {
  it('gives a new diagram of the same graph the positions of the ELK layout', async () => {
    const first = await mountDiagram('light', DIAMOND)
    expect(laidOutByElk(first.cy)).toBe(true)
    const elkPositions = positions(first.cy)
    const elkMessages = [...first.reported.messages]
    first.unmount()

    const second = await mountDiagram('light', DIAMOND)
    expect(laidOutByElk(second.cy)).toBe(false)
    expect(positions(second.cy)).toEqual(elkPositions)
    // The diagram shows and removes the progress message as it does for an ELK layout.
    expect(second.reported.messages).toEqual(elkMessages)
    second.unmount()
  })
})
