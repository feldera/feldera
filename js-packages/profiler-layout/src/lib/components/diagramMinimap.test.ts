// The minimap, which profiler-lib paints onto a canvas of its own (`minimapPicture.ts`) and moves the
// view from (`navigator.ts`). Its own suite is headless, so it stops at what the picture is made of and
// what it costs. Whether anything came out on screen, and whether a drag lands the view where it was
// aimed, is here.

import { describe, expect, it } from 'vitest'
import { DIAGRAM_PALETTES } from 'profiler-lib'
import {
  colorDistance,
  MANY,
  mountDiagram,
  settle,
  WITH_SOURCE,
  type Rgba
} from '../test-support/mountDiagram.js'

/** A palette color as a pixel, the palette holding them as `#rrggbb`. */
const hex = (color: string): Rgba => ({
  r: Number.parseInt(color.slice(1, 3), 16),
  g: Number.parseInt(color.slice(3, 5), 16),
  b: Number.parseInt(color.slice(5, 7), 16),
  a: 1
})

/** Center of the diagram's viewport, in model coordinates: where the minimap sends the view. */
const viewCenter = (cy: { extent(): { x1: number, y1: number, x2: number, y2: number } }) => {
  const view = cy.extent()
  return { x: (view.x1 + view.x2) / 2, y: (view.y1 + view.y2) / 2 }
}

describe('the minimap', () => {
  it('draws the whole circuit, out to the edges of the map', async () => {
    // The map is the graph's bounding box and nothing else, so a picture that fills it shows everything
    // on the graph. A press is read through that same mapping.
    const { minimap, cleanup } = await mountDiagram('light', MANY)
    const map = minimap()
    const ink = map.inked()

    expect(ink.count).toBeGreaterThan(100)
    // The chain of operators runs the whole height, give or take the band the graph's box keeps above a
    // node for the code chip that may be drawn there. The height and not the width: an operator's mark
    // is a dash of a fixed length, so across a map this narrow the picture says where the chain runs
    // rather than how wide the circuit is.
    expect(ink.y1).toBeLessThanOrEqual(4)
    expect(ink.y2).toBeGreaterThanOrEqual(map.size.h - 4)
    cleanup()
  })

  it('reaches the sides of the map where a region is what sets the width', async () => {
    // The other axis, on a circuit wide enough to have one: a region is the one thing drawn at its own
    // size, so it is what carries the picture out to the edges of a map that is its own box.
    const { minimap, cleanup } = await mountDiagram('light', WITH_SOURCE.profile)
    const map = minimap()
    const ink = map.inked()

    expect(ink.x1).toBeLessThanOrEqual(2)
    expect(ink.x2).toBeGreaterThanOrEqual(map.size.w - 2)
    cleanup()
  })

  it('is the shape of the circuit, at the area every map is drawn at', async () => {
    // `mapSize` is checked in profiler-lib; here, that the element really came out that size.
    const { cy, minimap, cleanup } = await mountDiagram('light', WITH_SOURCE.profile)
    const map = minimap()
    const graph = cy.elements().boundingBox()

    // Within a few pixels of the baseline square: the browser rounds a CSS pixel to a fraction of its
    // own, and the area multiplies both roundings.
    expect(map.size.w * map.size.h).toBeCloseTo(100 * 100, -1)
    expect(map.size.w / map.size.h).toBeCloseTo(graph.w / graph.h, 2)
    cleanup()
  })

  it('stands its frame off the picture on every side', async () => {
    // Room for a dot on the edge of the circuit, and for a view panned a little past it. The gap itself
    // is a design value and free to move, so what is pinned is that it is there and even.
    const { minimap, cleanup } = await mountDiagram('light', WITH_SOURCE.profile)
    const map = minimap()
    const frame = map.root.getBoundingClientRect()
    const picture = map.canvas.getBoundingClientRect()
    const sides = [
      picture.left - frame.left,
      picture.top - frame.top,
      frame.right - picture.right,
      frame.bottom - picture.bottom
    ]
    for (const side of sides) {
      expect(side).toBeGreaterThanOrEqual(4)
      expect(side).toBeCloseTo(sides[0]!, 1)
    }
    cleanup()
  })

  it('draws the operators in a gray the viewport outline reads over', async () => {
    // The dots have to be visible, and the outline over them too - which black dots under a black
    // outline would not be.
    const { minimap, cleanup } = await mountDiagram('light', WITH_SOURCE.profile)
    const map = minimap()
    const palette = DIAGRAM_PALETTES.light

    // A handful of pixels: three operators, each a dash of a fixed length.
    expect(map.painted(hex(palette.navigatorInk))).toBeGreaterThan(10)
    expect(map.painted(hex(palette.navigatorViewport))).toBe(0)
    cleanup()
  })

  it('moves the view to where it is pressed, and keeps up with the drag', async () => {
    // What a minimap is for: a press on it is an instruction to look there. The view lands centered on
    // the point pressed, to the pixel the press was aimed with.
    const { cy, minimap, cleanup } = await mountDiagram('light', MANY)
    const map = minimap()
    const graph = cy.elements().boundingBox()
    const model = (fraction: number) => ({ x: graph.x1 + graph.w / 2, y: graph.y1 + graph.h * fraction })
    /** A pointer carries whole pixels, and a pixel of this map is a percent of the circuit. */
    const perPixel = { x: graph.w / map.size.w, y: graph.h / map.size.h }
    const landedOn = (target: { x: number, y: number }, at: string) => {
      const center = viewCenter(cy)
      expect(Math.abs(center.y - target.y), at).toBeLessThanOrEqual(perPixel.y)
      expect(Math.abs(center.x - target.x), at).toBeLessThanOrEqual(perPixel.x)
    }

    for (const fraction of [0.8, 0.15, 0.5]) {
      await map.drag(map.at(model(fraction)))
      landedOn(model(fraction), `press at ${fraction}`)
    }

    // And the view follows every move of a drag, not only the press.
    await map.drag(map.at(model(0.1)), map.at(model(0.4)), map.at(model(0.9)))
    landedOn(model(0.9), 'end of the drag')
    cleanup()
  })

  it('outlines where the view is, and follows it as the diagram is panned', async () => {
    const { cy, minimap, cleanup } = await mountDiagram('light', MANY)
    const map = minimap()
    const outline = () => map.view.getBoundingClientRect()
    const before = outline()

    // The outline is placed against the circuit's own box, so a view centered on the middle of the
    // circuit is centered on the middle of the map - the frame's padding included nowhere.
    const picture = map.canvas.getBoundingClientRect()
    const center = viewCenter(cy)
    const graph = cy.elements().boundingBox()
    expect((outline().left + outline().right) / 2 - picture.left)
      .toBeCloseTo(((center.x - graph.x1) / graph.w) * picture.width, 0)

    cy.panBy({ x: 0, y: -200 })
    await settle()
    // Panning down the circuit moves the outline down the map and moves nothing else: the picture is
    // repainted only when a layout finishes.
    expect(outline().top).toBeGreaterThan(before.top)
    expect(map.inked()).toEqual(map.inked())

    // Zooming in leaves the outline over less of the circuit.
    const height = outline().height
    cy.zoom(cy.zoom() * 2)
    await settle()
    expect(outline().height).toBeLessThan(height)
    cleanup()
  })

  it('fits the whole circuit on a double click', async () => {
    const { cy, minimap, cleanup } = await mountDiagram('light', MANY)
    const graph = cy.elements().boundingBox()
    expect(cy.extent().y2 - cy.extent().y1).toBeLessThan(graph.h)

    minimap().root.dispatchEvent(new MouseEvent('dblclick', { bubbles: true }))
    await settle()
    const view = cy.extent()
    expect(view.y2 - view.y1).toBeGreaterThanOrEqual(graph.h)
    expect(view.x2 - view.x1).toBeGreaterThanOrEqual(graph.w)
    cleanup()
  })

  it('is repainted when the diagram changes palette', async () => {
    // The palette is baked into the picture, and the map sits over the diagram - where a light picture
    // on a dark diagram is the one thing on screen out of place.
    const { minimap, setTheme, cleanup } = await mountDiagram('light', MANY)
    const light = minimap()
    expect(light.painted(hex(DIAGRAM_PALETTES.light.navigatorInk))).toBeGreaterThan(20)

    await setTheme('dark')
    const dark = minimap()
    // The same canvas: the diagram restyles what is on screen, so the repaint is the palette change's
    // own doing and not a layout's.
    expect(dark.canvas).toBe(light.canvas)
    expect(dark.painted(hex(DIAGRAM_PALETTES.dark.navigatorInk))).toBeGreaterThan(20)
    expect(dark.painted(hex(DIAGRAM_PALETTES.light.navigatorInk))).toBe(0)
    // A dark gray on the light palette and a light one on the dark, which is what the repaint is for.
    expect(colorDistance(hex(DIAGRAM_PALETTES.light.navigatorInk), hex(DIAGRAM_PALETTES.dark.navigatorInk)))
      .toBeGreaterThan(100)
    cleanup()
  })
})
