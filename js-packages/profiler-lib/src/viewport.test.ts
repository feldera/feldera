// The view's policy, reachable here because it lives in a plugin: the decisions it makes at
// `layoutSettled` are arithmetic over a viewport and a node box, so they can be asked directly rather
// than only through a mounted diagram. Where the view actually lands on screen, and that a search moves
// there at the right zoom, stay in profiler-layout's `diagramView.test.ts` - those need a renderer.

import { describe, expect, it, vi } from 'vitest'

// The navigator builds DOM in its constructor, and this is about geometry, not the minimap.
vi.mock('./navigator.js', () => ({
    ViewNavigator: class {
        setOnDoubleClick() { }
        setTheme() { }
        setViewParameters() { }
    }
}))

import type { Core } from 'cytoscape'
import type { NodeId } from './profile.js'
import { Option } from './util.js'
import { Viewport } from './viewport.js'

/** A node's own box, in model coordinates. */
interface Box { x: number, y: number, w: number, h: number }

/** The viewport is 100x100 and shows model coordinates 0..100 at zoom 1, so a box is on screen exactly
 *  when it overlaps that square. */
const VIEW = { x1: 0, y1: 0, x2: 100, y2: 100 }

function cyStub(boxes: Record<string, Box>) {
    const pan = { x: 0, y: 0 }
    let zoom = 1
    const node = (box: Box) => ({
        nonempty: () => true,
        position: () => ({ x: box.x, y: box.y }),
        outerWidth: () => box.w,
        outerHeight: () => box.h,
        // At zoom 1 the rendered height is the model height.
        renderedHeight: () => box.h
    })
    return {
        pan: (to?: { x: number, y: number }) => (to === undefined ? pan : Object.assign(pan, to)),
        zoom: (to?: number | { level: number }) => {
            if (to === undefined) {
                return zoom
            }
            zoom = typeof to === 'number' ? to : to.level
            return zoom
        },
        width: () => 100,
        height: () => 100,
        // No container, so the minimap sync and the fit-the-graph zoom floor both bow out - neither has
        // anything to say about where a node lands.
        container: () => null,
        extent: () => VIEW,
        elements: () => ({ boundingBox: () => ({ w: 0, h: 0 }) }),
        getElementById: (id: string) => (boxes[id] ? node(boxes[id]!) : { nonempty: () => false }),
        destroyed: () => false,
        maxZoom: () => { },
        minZoom: () => { },
        on: () => { },
        fit: () => { }
    }
}

/** A viewport over `boxes`, past its first layout - the one that places the initial view, which every
 *  layout after leaves alone. */
function viewportOver(boxes: Record<string, Box>, firstNode?: NodeId) {
    const cy = cyStub(boxes)
    const viewport = new Viewport(
        cy as unknown as Core,
        {} as HTMLElement,
        () => firstNode,
        'light'
    )
    viewport.layoutSettled()
    return { cy, viewport }
}

/** Where the pan has to be for `box` to be centered in a 100x100 view at zoom 1. */
const centeredOn = (box: Box) => ({ x: 50 - box.x, y: 50 - box.y })

describe('the view after a layout that toggled a composite', () => {
    it('leaves the pan alone while any part of the composite is still on screen', () => {
        // Overlapping the right edge by five model px: still something to look at, so the view that the
        // user set is the view that stays.
        const box = { x: 105, y: 50, w: 20, h: 20 }
        const { cy, viewport } = viewportOver({ region: box })
        viewport.compositeToggled('region')
        viewport.layoutSettled()
        expect(cy.pan()).toEqual({ x: 0, y: 0 })
    })

    it('pans to it once the layout has pushed it off screen entirely', () => {
        // The same node one pixel further right, so its left edge clears the viewport.
        const box = { x: 111, y: 50, w: 20, h: 20 }
        const { cy, viewport } = viewportOver({ region: box })
        viewport.compositeToggled('region')
        viewport.layoutSettled()
        expect(cy.pan()).toEqual(centeredOn(box))
        // Only the pan: the zoom is the user's.
        expect(cy.zoom()).toBe(1)
    })

    it('gives an explicit request the last word over a toggle', () => {
        // Both pending at once: a search that expanded ancestors on the way to a node, and the toggle
        // that expanding them came down to. The user asked for one of the two.
        const asked = { x: 400, y: 400, w: 20, h: 20 }
        const { cy, viewport } = viewportOver({
            region: { x: 900, y: 900, w: 20, h: 20 },
            asked
        })
        viewport.compositeToggled('region')
        viewport.centerOnNextLayout(Option.some('asked'))
        viewport.layoutSettled()
        expect(cy.pan()).toEqual(centeredOn(asked))
    })

    it('forgets the toggle after the layout it belongs to', () => {
        const box = { x: 111, y: 50, w: 20, h: 20 }
        const { cy, viewport } = viewportOver({ region: box })
        viewport.compositeToggled('region')
        viewport.layoutSettled()

        // A later layout - a metric change, a resize - must not drag the view back to a node the user
        // has since panned away from.
        cy.pan({ x: 0, y: 0 })
        viewport.layoutSettled()
        expect(cy.pan()).toEqual({ x: 0, y: 0 })
    })

    it('ignores a node that is no longer drawn', () => {
        // Collapsing an ancestor can take the toggled node off the graph, and a missing element has no
        // position to ask for.
        const { cy, viewport } = viewportOver({})
        viewport.compositeToggled('gone')
        expect(() => viewport.layoutSettled()).not.toThrow()
        expect(cy.pan()).toEqual({ x: 0, y: 0 })
    })
})

describe('the first layout', () => {
    it('places the view on the node it is given, at the focus zoom', () => {
        const first = { x: 300, y: 300, w: 60, h: 20 }
        const cy = cyStub({ n0: first })
        const viewport = new Viewport(
            cy as unknown as Core,
            {} as HTMLElement,
            () => 'n0',
            'light'
        )
        viewport.layoutSettled()
        // 7.5 model px per 8 px of node box: the zoom a search stops at, so opening a profile and
        // finding a node in it leave the diagram at the same scale.
        const zoom = cy.zoom()
        expect(zoom).toBeCloseTo(7.5 / 8, 5)
        expect(cy.pan()).toEqual({ x: 50 - first.x * zoom, y: 50 - first.y * zoom })
    })

    it('leaves the view where it is on every layout after that', () => {
        const first = { x: 300, y: 300, w: 60, h: 20 }
        const { cy, viewport } = viewportOver({ n0: first }, 'n0')
        const placed = { ...cy.pan() }
        viewport.layoutSettled()
        expect(cy.pan()).toEqual(placed)
    })
})
