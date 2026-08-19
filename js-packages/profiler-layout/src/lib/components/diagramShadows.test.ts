// Node shadows are painted onto the cytoscape canvas by profiler-lib (`nodeShadow.ts`), through a
// renderer hook that only exists in a browser. See `test-support/mountDiagram.ts` for why these live
// here; what these check is what only pixels can say: that a shadow fades out, that it falls to the
// bottom right, that the selected node is glowing instead, and that the dark palette paints nothing
// around a node at all.

import { describe, expect, it } from 'vitest'
import { mountDiagram, settle } from '../test-support/mountDiagram.js'

describe('node shadows', () => {
  it('fades out with distance instead of ending like a border', async () => {
    const { cy, outside, cleanup } = await mountDiagram('light')
    cy.center(cy.$id('n1'))
    await settle()

    const near = outside('n1', 'below', 1).a
    const mid = outside('n1', 'below', 4).a
    const far = outside('n1', 'below', 10).a
    expect(near).toBeGreaterThan(0)
    expect(mid).toBeLessThan(near)
    expect(far).toBeLessThan(mid)
    cleanup()
  })

  it('falls to the bottom right, as one light source over the whole diagram', async () => {
    const { cy, outside, cleanup } = await mountDiagram('light')
    cy.center(cy.$id('n1'))
    await settle()

    expect(outside('n1', 'below', 2).a).toBeGreaterThan(outside('n1', 'above', 2).a)
    expect(outside('n1', 'right', 2).a).toBeGreaterThan(outside('n1', 'left', 2).a)
    cleanup()
  })

  it('turns into a colored glow around the node whose metrics are on display', async () => {
    const { cy, outside, cleanup } = await mountDiagram('light')
    const node = cy.$id('n1')
    cy.center(node)
    await settle()

    const shadow = outside('n1', 'below', 2)
    // A shadow is neutral: no channel stands out.
    expect(Math.abs(shadow.r - shadow.b)).toBeLessThan(8)

    node.emit('click')
    await settle()
    const glow = outside('n1', 'below', 2)
    // The glow is warm, and it reaches further than the shadow it stands in for.
    expect(glow.r - glow.b).toBeGreaterThan(40)
    expect(outside('n1', 'below', 10).a).toBeGreaterThan(0)
    // Only the clicked node glows.
    const neighbour = outside('n2', 'below', 2)
    expect(Math.abs(neighbour.r - neighbour.b)).toBeLessThan(8)
    cleanup()
  })

  it('follows a node found by search', async () => {
    // Searching moves the view to a node and reports on it, so it is selected in the same sense
    // as one that was clicked.
    const { cy, outside, diagram, cleanup } = await mountDiagram('light')
    cy.center(cy.$id('n2'))
    await settle()
    diagram.search('n2')
    await settle()
    const glow = outside('n2', 'below', 2)
    expect(glow.r - glow.b).toBeGreaterThan(40)
    const other = outside('n1', 'below', 2)
    expect(Math.abs(other.r - other.b)).toBeLessThan(8)
    cleanup()
  })

  it('paints nothing around a node in the dark palette', async () => {
    // The dark palette declines the ambient shadow: there a node is told from the canvas by its own
    // border, which a shadow behind it would only muddy. Nothing painted beside a node means an
    // untouched canvas, not a fainter shadow.
    const { cy, outside, cleanup } = await mountDiagram('dark')
    const node = cy.$id('n1')
    cy.center(node)
    await settle()

    for (const direction of ['below', 'right', 'above', 'left'] as const) {
      expect(outside('n1', direction, 2).a, direction).toBe(0)
    }
    cleanup()
  })

  it('still glows around the selected node in the dark palette', async () => {
    // Which is the point of declining the shadow rather than the mark: the one node the diagram is
    // reporting on is then the only node with anything painted around it.
    const { cy, outside, cleanup } = await mountDiagram('dark')
    const node = cy.$id('n1')
    cy.center(node)
    await settle()

    node.emit('click')
    await settle()
    const glow = outside('n1', 'below', 2)
    expect(glow.a).toBeGreaterThan(0)
    // Warm, as in the light palette: the mark is the same color in both.
    expect(glow.r - glow.b).toBeGreaterThan(40)
    // And its neighbour keeps its bare canvas.
    expect(outside('n2', 'below', 2).a).toBe(0)
    cleanup()
  })
})
