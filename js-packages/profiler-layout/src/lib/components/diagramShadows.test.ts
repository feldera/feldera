// The glow around the node whose metrics are on display is painted onto the cytoscape canvas by
// profiler-lib (`nodeShadow.ts`), through a renderer hook that only exists in a browser. See
// `test-support/mountDiagram.ts` for why these live here; what these check is what only pixels can
// say: that the glow is warm, that it fades out, that it is the marked node's alone, and that nothing
// is painted around any other node in either palette.

import { describe, expect, it } from 'vitest'
import { mountDiagram, settle } from '../test-support/mountDiagram.js'

describe('the selection glow', () => {
  it('paints nothing around a node until it is the one on display', async () => {
    // An operator is told from the canvas by its own border, so the canvas beside it stays untouched -
    // not painted with something faint.
    for (const theme of ['light', 'dark'] as const) {
      const { cy, outside, cleanup } = await mountDiagram(theme)
      cy.center(cy.$id('n1'))
      await settle()

      for (const direction of ['below', 'right', 'above', 'left'] as const) {
        expect(outside('n1', direction, 2).a, `${theme} ${direction}`).toBe(0)
      }
      cleanup()
    }
  })

  it('glows warm around the node whose metrics are on display, and fades out', async () => {
    const { cy, outside, cleanup } = await mountDiagram('light')
    const node = cy.$id('n1')
    cy.center(node)
    await settle()

    node.emit('click')
    await settle()
    const glow = outside('n1', 'below', 2)
    expect(glow.r - glow.b).toBeGreaterThan(40)
    // Fading out is what makes it read as light rather than as a second border.
    const near = outside('n1', 'below', 1).a
    const mid = outside('n1', 'below', 8).a
    const far = outside('n1', 'below', 20).a
    expect(near).toBeGreaterThan(0)
    expect(mid).toBeLessThan(near)
    expect(far).toBeLessThan(mid)
    // Only the clicked node glows.
    expect(outside('n2', 'below', 2).a).toBe(0)
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
    expect(outside('n1', 'below', 2).a).toBe(0)
    cleanup()
  })

  it('marks the node the same way in the dark palette', async () => {
    // The mark is the one thing painted around a node, so it carries the same color in both palettes
    // rather than being tuned to either.
    const { cy, outside, cleanup } = await mountDiagram('dark')
    const node = cy.$id('n1')
    cy.center(node)
    await settle()

    node.emit('click')
    await settle()
    const glow = outside('n1', 'below', 2)
    expect(glow.a).toBeGreaterThan(0)
    expect(glow.r - glow.b).toBeGreaterThan(40)
    expect(outside('n2', 'below', 2).a).toBe(0)
    cleanup()
  })
})
