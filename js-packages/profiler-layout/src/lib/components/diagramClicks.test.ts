// What a click on the diagram reports. profiler-lib decides that, but only a real pointer over a real
// canvas produces the events it decides from: a corner chip is hit-tested by hand, and an expanded
// region is hit-tested by cytoscape over everything it holds. See `test-support/mountDiagram.ts` for
// the harness, whose `reported` records what reached the application.

import { describe, expect, it } from 'vitest'
import { colorDistance, mountDiagram, settle, WITH_SOURCE } from '../test-support/mountDiagram.js'

const mount = (keepOpeningView = false) =>
  mountDiagram('light', WITH_SOURCE.profile, keepOpeningView, WITH_SOURCE.dataflow)

/** A rendered point inside the code chip, which rests on the node's top edge from outside: half a
 *  chip's height above that edge, and half a chip's width in from the right one. */
// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
const codePoint = (cy: any, id: string) => {
  const node = cy.$id(id)
  const { x, y } = node.renderedPosition()
  const zoom = cy.zoom()
  return {
    x: x + node.renderedOuterWidth() / 2 - 15 * zoom,
    y: y - node.renderedOuterHeight() / 2 - 8 * zoom
  }
}

/** A rendered point in the padding band down the left side of a region: inside the region, outside
 *  every node it holds, and nowhere near either of its chips. */
// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
const regionPoint = (cy: any, id: string) => {
  const node = cy.$id(id)
  const { x, y } = node.renderedPosition()
  return { x: x - node.renderedOuterWidth() / 2 + 5 * cy.zoom(), y }
}

/** Press at a point and drag `by` pixels from it, as a mouse with a hand on it does. */
const drag = async (
  pointer: (type: 'mousemove' | 'mousedown' | 'mouseup', x: number, y: number) => void,
  from: { x: number, y: number },
  by: { x: number, y: number }
) => {
  pointer('mousemove', from.x, from.y)
  pointer('mousedown', from.x, from.y)
  pointer('mousemove', from.x + by.x / 2, from.y + by.y / 2)
  pointer('mousemove', from.x + by.x, from.y + by.y)
  pointer('mouseup', from.x + by.x, from.y + by.y)
  await settle()
}

// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
const highlighted = (cy: any) =>
  cy.edges('.highlight-forward, .highlight-backward').map((e: { id(): string }) => e.id())

describe('a press on a corner chip', () => {
  it('reports on the node the chip belongs to, not on what is behind it', async () => {
    // The code chip hangs above its node's top edge, outside the shape cytoscape hit-tests, so the press
    // lands on the region around that node too, which would report the region while the source of the
    // node the chip belongs to opens beside it.
    const { cy, press, reported, cleanup } = await mount()
    expect(cy.$id('n1').data('has_source')).toBe(true)
    expect(cy.$id('region').isParent()).toBe(true)
    cy.center(cy.$id('n1'))
    await settle()

    const chip = codePoint(cy, 'n1')
    await press(chip.x, chip.y)
    // The source lookup a press on the code chip asks for, which reaches consumers as the one a
    // double click on an operator asks for.
    expect(reported.doubleClicks).toEqual([{ nodeId: 'n1', type: 'leaf' }])
    // And the node the chip belongs to is the node reported on, marked and traced - the press is a
    // click on it as much as it is a button.
    expect(reported.nodeClicks).toEqual(['n1'])
    expect(reported.attributes.filter((a) => a.isSticky)).toEqual([
      { nodeId: 'n1', isSticky: true }
    ])
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])
    expect(highlighted(cy).length).toBeGreaterThan(0)
    cleanup()
  })

  it('leaves the counter chip a toggle, reporting nothing', async () => {
    // Pressing the counter is what expands or collapses the composite, and the graph is rebuilt under
    // whatever it would have reported - so it reports nothing rather than something with one frame to
    // live.
    const { cy, press, reported, cleanup } = await mount()
    const region = cy.$id('region')
    cy.center(region)
    await settle()

    const { x, y } = region.renderedPosition()
    const counter = {
      x: x + region.renderedOuterWidth() / 2 - 6 * cy.zoom(),
      y: y - region.renderedOuterHeight() / 2 + 8 * cy.zoom()
    }
    await press(counter.x, counter.y)
    expect(cy.$id('region').isParent()).toBe(false)
    expect(reported.nodeClicks).toEqual([])
    expect(reported.attributes.filter((a) => a.isSticky)).toEqual([])
    cleanup()
  })

  it('paints no press feedback on what it is drawn over', async () => {
    // Cytoscape marks whatever it hit-tested under a press as active and paints its own gray overlay
    // over the whole of it - a region reporting a press aimed at a button that happens to be drawn in
    // its top band, and the one thing the user sees while the button is held down.
    const { cy, pixelAt, pointer, reported, cleanup } = await mount()
    cy.center(cy.$id('n1'))
    await settle()
    // In the region's padding band, well inside the overlay and outside every node it holds.
    const region = cy.$id('region')
    const band = {
      x: region.renderedPosition().x - region.renderedOuterWidth() / 2 + 5 * cy.zoom(),
      y: region.renderedPosition().y
    }
    const before = pixelAt(band.x, band.y)

    const chip = codePoint(cy, 'n1')
    pointer('mousemove', chip.x, chip.y)
    pointer('mousedown', chip.x, chip.y)
    await settle()
    expect(cy.nodes(':active').map((n: { id(): string }) => n.id())).toEqual([])
    expect(colorDistance(pixelAt(band.x, band.y), before)).toBe(0)

    // And the press is a press: letting go of it presses the button, so none of the above is a dead
    // gesture that nothing would have reacted to anyway.
    pointer('mouseup', chip.x, chip.y)
    await settle()
    expect(reported.nodeClicks).toEqual(['n1'])
    cleanup()
  })

  it('does not pan the view from a chip resting over the background', async () => {
    // A code chip on a node that is not inside a region hangs over the empty canvas, which cytoscape
    // hit-tests as a press on the background: it answers that with a gray dot of its own and, as soon
    // as the hand moves, by panning the whole diagram out from under the button.
    const { cy, pointer, reported, cleanup } = await mount()
    expect(cy.$id('n0').data('has_source')).toBe(true)
    expect(cy.$id('n0').isChild()).toBe(false)
    cy.center(cy.$id('n0'))
    await settle()
    const pan = { ...cy.pan() }

    await drag(pointer, codePoint(cy, 'n0'), { x: 90, y: 40 })
    expect(cy.pan()).toEqual(pan)
    // A press dragged off the button is a cancelled press, as it is for every other button.
    expect(reported.doubleClicks).toEqual([])
    expect(reported.nodeClicks).toEqual([])
    cleanup()
  })

  it('does not drag what cytoscape hit-tests behind it', async () => {
    // Cytoscape decides at the press what a gesture drags, by the node's own shape, so a press on the
    // code chip would take hold of the region behind it and carry it, and every node inside it, off its
    // laid-out position on the smallest movement of the hand.
    const { cy, pointer, cleanup } = await mount()
    cy.center(cy.$id('n1'))
    await settle()
    const before = { region: { ...cy.$id('region').position() }, n1: { ...cy.$id('n1').position() } }

    await drag(pointer, codePoint(cy, 'n1'), { x: 80, y: 60 })
    expect(cy.$id('region').position()).toEqual(before.region)
    expect(cy.$id('n1').position()).toEqual(before.n1)

    // A node dragged by its own body still moves: what a chip press takes away, it takes away for
    // the press it belongs to and nothing else.
    const outside = cy.$id('n0')
    const start = { ...outside.position() }
    await drag(pointer, outside.renderedPosition(), { x: 40, y: 30 })
    expect(outside.position()).not.toEqual(start)
    cleanup()
  })

  it('does not select what cytoscape hit-tests behind it', async () => {
    // Cytoscape's own selection, which the diagram does not use and does not paint - but it decides
    // what a later drag carries, so a chip press must not hand it the region either.
    const { cy, press, cleanup } = await mount()
    cy.center(cy.$id('n1'))
    await settle()

    const chip = codePoint(cy, 'n1')
    await press(chip.x, chip.y)
    expect(cy.$('node:selected').map((n: { id(): string }) => n.id())).toEqual([])

    // And a press on the region itself still selects it, as any press on a node does.
    const point = regionPoint(cy, 'region')
    await press(point.x, point.y)
    expect(cy.$('node:selected').map((n: { id(): string }) => n.id())).toEqual(['region'])
    cleanup()
  })

  it('leaves the region it is drawn over expanded when pressed twice', async () => {
    // Two presses inside the double click interval, which cytoscape reports as a double click on
    // whatever is behind the chip - and a double click on a region collapses it.
    const { cy, press, reported, cleanup } = await mount()
    cy.center(cy.$id('n1'))
    await settle()

    const chip = codePoint(cy, 'n1')
    await press(chip.x, chip.y)
    await press(chip.x, chip.y)
    await settle()
    expect(cy.$id('region').isParent()).toBe(true)
    expect(reported.doubleClicks).toEqual([
      { nodeId: 'n1', type: 'leaf' },
      { nodeId: 'n1', type: 'leaf' }
    ])
    cleanup()
  })
})

describe('a click on an expanded region', () => {
  it('reports the metrics of the region', async () => {
    // A region holds the aggregate of everything inside it, and clicking it is how that is asked for.
    // The guard that keeps a hover from reporting every region the pointer crosses must not swallow the
    // click as well.
    const { cy, press, reported, cleanup } = await mount(true)
    const point = regionPoint(cy, 'region')
    await press(point.x, point.y)

    expect(reported.nodeClicks).toEqual(['region'])
    expect(reported.attributes.filter((a) => a.isSticky)).toEqual([
      { nodeId: 'region', isSticky: true }
    ])
    cleanup()
  })

  it('marks nothing and colors no edge, which an operator does', async () => {
    // A region stands in for every node inside it, so tracing it would color every edge in it, and
    // there is nothing for the mark to draw on - a region casts no shadow, glow included.
    const { cy, press, cleanup } = await mount(true)
    const point = regionPoint(cy, 'region')
    await press(point.x, point.y)
    expect(cy.nodes('.selected-node').length).toBe(0)
    expect(highlighted(cy)).toEqual([])

    // The operator inside it, for contrast: the mark and the trace are what a click on a node does, the
    // region being the exception.
    const node = cy.$id('n1')
    await press(node.renderedPosition().x, node.renderedPosition().y)
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])
    expect(highlighted(cy).length).toBeGreaterThan(0)
    cleanup()
  })

  it('does not take over from a report already asked for', async () => {
    // The hover path and the click path end in the same place, so the one thing that keeps them apart
    // is that a report asked for by a click stays until it is dismissed.
    const { cy, press, reported, cleanup } = await mount(true)
    const node = cy.$id('n1')
    node.emit('mouseover')
    await settle()
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'n1', isSticky: false })

    const point = regionPoint(cy, 'region')
    await press(point.x, point.y)
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'region', isSticky: true })

    // A pointer crossing an operator afterwards leaves the region's report where it is.
    node.emit('mouseover')
    await settle()
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'region', isSticky: true })
    cleanup()
  })

  it('leaves the pointer free to mark and trace the operators it crosses', async () => {
    // The report holds no mark and colors no edge, so it leaves the diagram itself blank. Silencing the
    // pointer on top of that would turn every hover in the graph off, and the click would look like it
    // had done nothing at all.
    const { cy, press, reported, cleanup } = await mount(true)
    const point = regionPoint(cy, 'region')
    await press(point.x, point.y)
    expect(cy.nodes('.selected-node').length).toBe(0)

    const node = cy.$id('n1')
    node.emit('mouseover')
    await settle()
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])
    expect(highlighted(cy).length).toBeGreaterThan(0)
    // And it took nothing away from what the click reported, which stays until it is dismissed.
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'region', isSticky: true })

    // The mark goes with the pointer, the report stays behind it.
    node.emit('mouseout')
    await settle()
    expect(cy.nodes('.selected-node').length).toBe(0)
    expect(highlighted(cy)).toEqual([])
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'region', isSticky: true })
    cleanup()
  })

  it('which a click on an operator does not, its own mark being what a hover would take', async () => {
    // The other half of the rule: a report that does hold a mark keeps it against the pointer, or
    // clicking an operator would only mark it until the mouse moved on.
    const { cy, press, cleanup } = await mount(true)
    const node = cy.$id('n1')
    await press(node.renderedPosition().x, node.renderedPosition().y)
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])

    cy.$id('n2').emit('mouseover')
    await settle()
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])
    cleanup()
  })

  it('still says nothing when the pointer only crosses it', async () => {
    // The reason the guard is there: a region covers everything it holds, so following the pointer
    // into one would report the region on the way to whatever the user was heading for.
    const { cy, pointer, reported, cleanup } = await mount(true)
    const point = regionPoint(cy, 'region')
    pointer('mousemove', point.x, point.y)
    cy.$id('region').emit('mouseover')
    await settle()
    expect(reported.attributes.filter((a) => a.nodeId !== null)).toEqual([])
    cleanup()
  })
})
