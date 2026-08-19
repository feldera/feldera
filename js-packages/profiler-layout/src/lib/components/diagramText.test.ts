// A node's text is painted onto the cytoscape canvas by profiler-lib (`nodeText.ts`): the node id in
// semibold, then the operator name in a muted color, which one cytoscape label could not carry. See
// `test-support/mountDiagram.ts` for why these live here. What only pixels can say is checked here -
// that the text is drawn at all, that the two runs are drawn differently, and that a collapsed
// composite keeps its text in the first of its two rows.

import { describe, expect, it } from 'vitest'
import {
  COMPOSITE,
  colorDistance,
  mountDiagram,
  type Rgba,
  settle,
  WIDE_REGION
} from '../test-support/mountDiagram.js'

/** The columns of a band, brightest-standing-out pixel first. */
const strongest = (columns: Array<{ distance: number }>) =>
  Math.max(...columns.map((c) => c.distance))

/** Ink of the leading and trailing fifth of whatever text is drawn in `columns`: the first belongs to
 *  the id, the last to the operator name, as long as the operator name is the longer of the two. */
const runs = (columns: Array<{ x: number, distance: number, color: Rgba }>) => {
  const ink = columns.filter((c) => c.distance > 30)
  const fifth = Math.max(1, Math.floor(ink.length / 5))
  return { ink, id: ink.slice(0, fifth), operator: ink.slice(-fifth) }
}

const WHITE: Rgba = { r: 255, g: 255, b: 255, a: 1 }

describe('node text', () => {
  it('draws the text itself, the cytoscape label being transparent', async () => {
    // The label is kept transparent and only measured, so nothing but the painter puts ink on a node.
    const { cy, inkColumns, cleanup } = await mountDiagram('light')
    cy.center(cy.$id('n1'))
    await settle()

    // A quarter down the node is the middle of its only row.
    expect(strongest(inkColumns('n1', 0.5, WHITE))).toBeGreaterThan(200)
    cleanup()
  })

  it('sets the id apart from the operator name', async () => {
    const { cy, inkColumns, cleanup } = await mountDiagram('light')
    const node = cy.$id('n1')
    cy.center(node)
    await settle()

    const { ink, id, operator } = runs(inkColumns('n1', 0.5, WHITE))
    expect(ink.length).toBeGreaterThan(10)
    // The id is semibold and drawn in the plain text color; the operator name is lighter in both
    // senses, so the strongest ink under it stays well short of the id's.
    expect(strongest(id)).toBeGreaterThan(strongest(operator) + 60)
    cleanup()
  })

  it('draws both runs in the dark palette too', async () => {
    const { cy, inkColumns, nodeFill, cleanup } = await mountDiagram('dark')
    const node = cy.$id('n1')
    cy.center(node)
    await settle()

    // The surface is the node's own fill here, not the page.
    const { id, operator } = runs(inkColumns('n1', 0.5, nodeFill('n1')))
    expect(strongest(id)).toBeGreaterThan(strongest(operator) + 60)
    cleanup()
  })

  it('keeps the text of a collapsed composite in its second row, below the counter chip', async () => {
    // A circuit this small renders its regions expanded, so the composite has to be collapsed first.
    const { cy, inkColumns, toggle, cleanup } = await mountDiagram('light', COMPOSITE)
    await toggle('region')
    const composite = cy.$id('region')
    expect(composite.isParent()).toBe(false)
    cy.center(composite)
    await settle()

    // The chip in the upper row, text in the lower one, and both of them ink.
    const upper = runs(inkColumns('region', 0.2, WHITE))
    const lower = runs(inkColumns('region', 0.72, WHITE))
    expect(strongest(upper.ink)).toBeGreaterThan(30)
    expect(strongest(lower.ink)).toBeGreaterThan(200)
    // The chip is a pill in the top right corner, so the upper row's ink starts well right of where the
    // text row's does.
    expect(Math.min(...upper.ink.map((c) => c.x))).toBeGreaterThan(
      Math.min(...lower.ink.map((c) => c.x))
    )
    // And the text really is in the second row rather than centered over both: on the node's own center
    // line, left of the chip, nothing is drawn at all.
    const centerX = composite.renderedPosition().x
    const middle = inkColumns('region', 0.5, WHITE, 0.12).filter((c) => c.x < centerX)
    expect(strongest(middle)).toBeLessThan(40)
    cleanup()
  })

  it('keeps a region wide enough for the name drawn in its top band', async () => {
    // A cytoscape parent is sized by the nodes it holds and ignores its own label, so a region around one
    // short operator would be narrower than its name, leaving the name running past both borders.
    const { cy, pixelAt, toggle, cleanup } = await mountDiagram('light', WIDE_REGION)
    const region = cy.$id('region')
    expect(region.isParent()).toBe(true)
    cy.center(region)
    await settle()

    const { x, y } = region.renderedPosition()
    const halfWidth = region.renderedOuterWidth() / 2
    // The row the name is drawn on, a chip's height below the region's top edge. Nothing the region
    // holds reaches it, so any pixel painted beside the region there is name that did not fit.
    const row = Math.round(y - region.renderedOuterHeight() / 2 + 9 * cy.zoom())
    const paintedIn = (from: number, to: number) => {
      for (let px = Math.round(from); px <= Math.round(to); px++) {
        for (let py = row - 4; py <= row + 4; py++) {
          if (pixelAt(px, py).a > 0) {
            return { x: px, y: py }
          }
        }
      }
      return null
    }
    expect(paintedIn(x - halfWidth - 80, x - halfWidth - 4)).toBeNull()
    expect(paintedIn(x + halfWidth + 4, x + halfWidth + 80)).toBeNull()
    // And the width is the name's: at least what the same node measures collapsed, which is where
    // cytoscape sizes it from its label directly.
    const expanded = region.renderedOuterWidth()
    await toggle('region')
    expect(cy.$id('region').isParent()).toBe(false)
    expect(expanded).toBeGreaterThan(cy.$id('region').renderedOuterWidth())
    cleanup()
  })
})
