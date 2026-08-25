// What the view does around a layout: where a profile opens, what a search moves to, and that neither
// expanding nor collapsing moves the ground under the user. These only mean anything on a mounted
// diagram - the viewport is a rendered thing, and the zoom levels involved are clamped by what fitting
// the graph on screen takes, which is why they run over a graph too tall to fit.

import { describe, expect, it } from 'vitest'
import { COMPOSITE, MANY, mountDiagram, settle } from '../test-support/mountDiagram.js'

/** Rendered height of a node's own box, which is what the view's zoom is chosen by. */
// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
const height = (cy: any, id: string) => cy.$id(id).renderedHeight()

describe('the view', () => {
  it('opens on the first node of the profile, at the zoom a search moves to', async () => {
    const { cy, container, cleanup } = await mountDiagram('light', MANY, true)
    await settle()

    // Not fitted to the whole circuit: a large one is unreadable at the zoom that takes.
    const box = cy.elements().boundingBox()
    expect(box.h * cy.zoom()).toBeGreaterThan(container.clientHeight)
    // On the first node, which the DOWN layout puts at the top.
    const first = cy.$id('n0').renderedPosition()
    expect(first.x).toBeCloseTo(container.clientWidth / 2, -1)
    expect(first.y).toBeCloseTo(container.clientHeight / 2, -1)
    // And a node is a handful of pixels tall there, the same as after a search.
    const opened = height(cy, 'n0')
    expect(opened).toBeGreaterThan(5)
    expect(opened).toBeLessThan(10)
    cleanup()
  })

  it('moves to a searched node without zooming past that same height', async () => {
    const { cy, container, diagram, cleanup } = await mountDiagram('light', MANY, true)
    await settle()
    const opened = height(cy, 'n0')

    // From the opening view, where a node is already tall enough, only the pan moves.
    diagram.search('n40')
    await settle()
    expect(height(cy, 'n40')).toBeCloseTo(opened, 5)
    expect(cy.$id('n40').renderedPosition().y).toBeCloseTo(container.clientHeight / 2, -1)

    // And from further out, the node is brought back to that height rather than to a readable one.
    cy.zoom(cy.minZoom())
    await settle()
    expect(height(cy, 'n20')).toBeLessThan(opened)
    diagram.search('n20')
    await settle()
    expect(height(cy, 'n20')).toBeCloseTo(opened, 0)
    cleanup()
  })

  it('leaves the view where it is when a composite is expanded or collapsed', async () => {
    // Centering the view on the composite instead would move everything the user was looking at, the
    // node they pressed included.
    const { cy, toggle, cleanup } = await mountDiagram('light', COMPOSITE)
    await settle()
    const before = { pan: { ...cy.pan() }, zoom: cy.zoom() }

    await toggle('region')
    expect(cy.$id('region').isParent()).toBe(false)
    expect(cy.pan()).toEqual(before.pan)
    expect(cy.zoom()).toBe(before.zoom)

    await toggle('region')
    expect(cy.$id('region').isParent()).toBe(true)
    expect(cy.pan()).toEqual(before.pan)
    expect(cy.zoom()).toBe(before.zoom)
    cleanup()
  })

  it('pans to a toggled composite when the new layout leaves it off screen', async () => {
    // Leaving the view alone is only right while the node is still in it. A layout can move a composite
    // that grew or shrank clear off screen, and the user is then looking at somewhere else entirely,
    // with nothing to say where the node they pressed went.
    const { cy, container, toggle, cleanup } = await mountDiagram('light', COMPOSITE)
    await settle()
    const zoom = cy.zoom()

    // A couple of screens away, so no part of the region is anywhere near the view.
    cy.panBy({ x: -3 * container.clientWidth, y: -3 * container.clientHeight })
    expect(cy.$id('region').renderedPosition().x).toBeLessThan(0)

    await toggle('region')
    const after = cy.$id('region').renderedPosition()
    expect(after.x).toBeCloseTo(container.clientWidth / 2, -1)
    expect(after.y).toBeCloseTo(container.clientHeight / 2, -1)
    // The pan is all that moved: the zoom is the user's.
    expect(cy.zoom()).toBe(zoom)
    cleanup()
  })

  it('holds the last layout on screen while the next one is computed', async () => {
    const { cy, container, cleanup } = await mountDiagram('light', COMPOSITE)
    await settle()
    const held = () => container.querySelector('div[style*="background-image"]') !== null
    const canvases = () => Array.from(container.querySelectorAll('canvas'))
    expect(held()).toBe(false)

    // The picture goes up before the graph is touched, so it is already there when the toggle returns.
    cy.$id('region').emit('dblclick')
    expect(held()).toBe(true)
    expect(canvases().every((c) => c.style.visibility === 'hidden')).toBe(true)

    await settle()
    await settle()
    // And comes down once the layout it was covering is on screen.
    expect(held()).toBe(false)
    expect(canvases().every((c) => c.style.visibility === '')).toBe(true)
    cleanup()
  })
})
