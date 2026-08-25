// The corner chips of a node are buttons (profiler-lib's `chipButtons.ts`), which cytoscape itself
// knows nothing about: they are background images, and their boxes are hit-tested by hand. What only
// a real browser can say is checked here - that a real pointer over the chip reaches that hit test,
// that the cursor says the chip is pressable, and that pressing it expands or collapses the composite.
// Which of the two controls a chip shows, and that a control keeps the width of the count it stands
// in for, are pinned in profiler-lib's own suites.
//
// The counter chip is the one exercised: it is the only chip the fixtures can carry, since a code chip
// needs source positions, which arrive from a dataflow graph rather than from a profile.

import { describe, expect, it } from 'vitest'
import { COMPOSITE, mountDiagram, type Rgba, settle } from '../test-support/mountDiagram.js'

const WHITE: Rgba = { r: 255, g: 255, b: 255, a: 1 }

/** A rendered point inside the counter chip, which sits just inside the node's top right corner:
 *  20 graph px tall from a pixel below the top edge, and a pill's width in from the right one. */
// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
const counterPoint = (cy: any, id: string) => {
  const node = cy.$id(id)
  const { x, y } = node.renderedPosition()
  const zoom = cy.zoom()
  return {
    x: x + node.renderedOuterWidth() / 2 - 6 * zoom,
    y: y - node.renderedOuterHeight() / 2 + 10 * zoom
  }
}

/** Ink of the counter row of a collapsed composite, the row above its text: the chip is the only thing
 *  drawn there. */
const counterInk = (
  inkColumns: (
    id: string,
    band: number,
    surface: Rgba,
    bandHeight?: number,
    inset?: number
  ) => Array<{ x: number, distance: number }>
  // Sampled to within a couple of pixels of the node's edges, since the chip sits inside its padding.
) => inkColumns('region', 0.2, WHITE, 0.25, 2).filter((c) => c.distance > 30)

describe('chip buttons', () => {
  it('draws the network icon beside the count', async () => {
    // The icon is an SVG group scaled down inside the chip image, so only a real render says it came
    // out as strokes on the pill rather than as nothing at all.
    const { cy, inkColumns, toggle, cleanup } = await mountDiagram('light', COMPOSITE)
    // Collapsed, so the counter chip has a row of the node to itself.
    await toggle('region')
    cy.center(cy.$id('region'))
    await settle()

    // The pill's own border stands out from the node fill as well, so only the darker glyph ink counts.
    const ink = inkColumns('region', 0.2, WHITE, 0.25, 2).filter((c) => c.distance > 300)
    expect(ink.length).toBeGreaterThan(2)
    const span = (Math.max(...ink.map((c) => c.x)) - Math.min(...ink.map((c) => c.x))) / cy.zoom()
    // A single digit is some 7 graph px wide, and the icon and the gap after it are 14 more.
    expect(span).toBeGreaterThan(15)
    cleanup()
  })

  it('points the cursor at the chip and nowhere else', async () => {
    const { cy, container, pointer, cleanup } = await mountDiagram('light', COMPOSITE)
    const region = cy.$id('region')
    cy.center(region)
    await settle()

    const chip = counterPoint(cy, 'region')
    pointer('mousemove', chip.x, chip.y)
    expect(container.style.cursor).toBe('pointer')

    // The middle of the region, which is the child node and the space around it.
    const { x, y } = region.renderedPosition()
    pointer('mousemove', x, y)
    expect(container.style.cursor).toBe('')
    cleanup()
  })

  it('collapses a region when its counter is pressed, and expands it again', async () => {
    const { cy, press, cleanup } = await mountDiagram('light', COMPOSITE)
    const region = cy.$id('region')
    cy.center(region)
    await settle()
    expect(region.isParent()).toBe(true)

    await press(counterPoint(cy, 'region').x, counterPoint(cy, 'region').y)
    expect(cy.$id('region').isParent()).toBe(false)

    // The chip is in the same corner of the collapsed node, so the same button undoes it.
    cy.center(cy.$id('region'))
    await settle()
    await press(counterPoint(cy, 'region').x, counterPoint(cy, 'region').y)
    expect(cy.$id('region').isParent()).toBe(true)
    cleanup()
  })

  it('leaves the node alone when pressed elsewhere', async () => {
    // The press has to reach the chip, not merely the node it sits on.
    const { cy, press, cleanup } = await mountDiagram('light', COMPOSITE)
    const region = cy.$id('region')
    cy.center(region)
    await settle()

    const { x, y } = region.renderedPosition()
    await press(x, y)
    expect(cy.$id('region').isParent()).toBe(true)
    cleanup()
  })

  it('draws a control in place of the count while the composite is hovered', async () => {
    const { cy, inkColumns, pointer, toggle, cleanup } = await mountDiagram('light', COMPOSITE)
    // Collapsed, so the counter chip has a row of the node to itself.
    await toggle('region')
    cy.center(cy.$id('region'))
    await settle()

    const count = counterInk(inkColumns)
    expect(count.length).toBeGreaterThan(2)

    pointer('mousemove', counterPoint(cy, 'region').x, counterPoint(cy, 'region').y)
    await settle()
    const control = counterInk(inkColumns)
    expect(control.length).toBeGreaterThan(2)
    // Something else is drawn in that row now - by a margin, since repainting the same chip does not
    // reproduce every antialiased pixel of it.
    const mass = (ink: Array<{ distance: number }>) => ink.reduce((sum, c) => sum + c.distance, 0)
    expect(Math.abs(mass(control) - mass(count))).toBeGreaterThan(mass(count) * 0.05)
    expect(control.length).not.toBe(count.length)
    // But in the same place and at the same width: a button that resized under the pointer holding
    // it would slip out from under that pointer.
    const span = (ink: Array<{ x: number }>) => ({
      left: Math.min(...ink.map((c) => c.x)),
      right: Math.max(...ink.map((c) => c.x))
    })
    expect(Math.abs(span(control).right - span(count).right)).toBeLessThanOrEqual(4)
    expect(Math.abs(span(control).left - span(count).left)).toBeLessThanOrEqual(4)
    cleanup()
  })

  it('shows the count again once the pointer leaves', async () => {
    const { cy, inkColumns, pointer, toggle, cleanup } = await mountDiagram('light', COMPOSITE)
    await toggle('region')
    cy.center(cy.$id('region'))
    await settle()
    const mass = (ink: Array<{ distance: number }>) => ink.reduce((sum, c) => sum + c.distance, 0)
    const count = mass(counterInk(inkColumns))

    pointer('mousemove', counterPoint(cy, 'region').x, counterPoint(cy, 'region').y)
    await settle()
    const hovered = mass(counterInk(inkColumns))
    expect(Math.abs(hovered - count)).toBeGreaterThan(count * 0.02)

    // Off the node, onto the empty canvas around the graph. Repainting a chip does not reproduce every
    // antialiased pixel of it, so what is asked of the count is that it comes back, not that it comes
    // back to the pixel.
    pointer('mousemove', 5, 5)
    await settle()
    expect(Math.abs(mass(counterInk(inkColumns)) - count)).toBeLessThan(count * 0.02)
    cleanup()
  })
})
