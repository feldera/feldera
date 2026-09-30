// Unit tests for how `Viewport` moves and zooms the view when a layout finishes. To decide this,
// `Viewport` only reads positions and sizes: the visible area and the nodes' boxes. So these tests give
// it a cytoscape stub with fixed numbers instead of a real diagram. The browser tests in
// profiler-layout check where the view actually lands on screen, because that needs a renderer.

import { describe, expect, it, vi } from 'vitest'

// Replace the minimap with a mock, so that these tests do not need a DOM. The mock adds the name of
// each `showGraph` and `showView` call to `minimap`. The minimap test at the end of this file reads
// that list.
const minimap = vi.hoisted(() => [] as string[])
vi.mock('./navigator.js', () => ({
    ViewNavigator: class {
        setOnDoubleClick() { }
        setOnMoveTo() { }
        setTheme() { }
        showGraph() { minimap.push('showGraph') }
        showView() { minimap.push('showView') }
    }
}))

import type { Core } from 'cytoscape'
import type { NodeId } from './profile.js'
import { Option } from './util.js'
import { Viewport } from './viewport.js'

/** A node box: center `x`, `y` and size `w`, `h` (`outerWidth()` / `outerHeight()`), in model
 *  coordinates. */
interface Box { x: number, y: number, w: number, h: number }

/** The view is 100x100 px and shows model coordinates 0..100 at zoom 1. A box is on screen only when
 *  it overlaps this square. */
const VIEW = { x1: 0, y1: 0, x2: 100, y2: 100 }

/** A cytoscape stub that holds `boxes`. `graph` is the size of the full graph. If it is given, the stub
 *  also has a container, so `Viewport` can calculate the zoom that fits the graph, which is the minimum
 *  zoom. If it is not given, there is no minimum zoom, so a test can check where a node lands without
 *  that limit. */
function cyStub(boxes: Record<string, Box>, graph?: { w: number, h: number }) {
    const pan = { x: 0, y: 0 }
    let zoom = 1
    let floor = 0
    let ceiling = Infinity
    let fitted = 0
    const handlers: Array<{ events: string, run: () => void }> = []
    const node = (box: Box) => ({
        nonempty: () => true,
        position: () => ({ x: box.x, y: box.y }),
        outerWidth: () => box.w,
        outerHeight: () => box.h
    })
    return {
        pan: (to?: { x: number, y: number }) => (to === undefined ? pan : Object.assign(pan, to)),
        zoom: (to?: number | { level: number }) => {
            if (to === undefined) {
                return zoom
            }
            // Cytoscape keeps the zoom between the minimum and maximum set below. The stub does the
            // same, because some tests check these limits.
            const level = typeof to === 'number' ? to : to.level
            zoom = Math.min(ceiling, Math.max(floor, level))
            return zoom
        },
        width: () => 100,
        height: () => 100,
        container: () =>
            graph === undefined
                ? null
                : ({ getBoundingClientRect: () => ({ width: 100, height: 100 }) } as unknown as HTMLElement),
        extent: () => VIEW,
        elements: () => ({ boundingBox: () => graph ?? { w: 0, h: 0 } }),
        getElementById: (id: string) => (boxes[id] ? node(boxes[id]!) : { nonempty: () => false }),
        destroyed: () => false,
        maxZoom: (level: number) => { ceiling = level },
        minZoom: (level: number) => { floor = level },
        on: (events: string, run: () => void) => handlers.push({ events, run }),
        fit: () => { fitted += 1 },
        fitCount: () => fitted,
        /** Not cytoscape's `emit`. Sends an event to the handlers that `Viewport` registered. */
        fire: (event: string) => {
            for (const handler of handlers) {
                if (handler.events.split(' ').includes(event)) {
                    handler.run()
                }
            }
        }
    }
}

/** A viewport over `boxes`, after its first layout. The first layout places the initial view, and
 *  later layouts do not do that again. */
function viewportOver(boxes: Record<string, Box>, firstNode?: NodeId, graph?: { w: number, h: number }) {
    const cy = cyStub(boxes, graph)
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

describe('the view after a layout that toggled a circuit region', () => {
    it('leaves the pan alone while any part of the circuit region is still on screen', () => {
        // The box overlaps the right edge of the view by 5 units. A part of the region is still
        // visible, so the view that the user set does not change.
        const box = { x: 105, y: 50, w: 20, h: 20 }
        const { cy, viewport } = viewportOver({ region: box })
        viewport.circuitRegionToggled('region')
        viewport.layoutSettled()
        expect(cy.pan()).toEqual({ x: 0, y: 0 })
    })

    it('pans to it once the layout has pushed it off screen entirely', () => {
        // The same box, 6 units further right. Its left edge is now 1 unit past the right edge of the
        // view.
        const box = { x: 111, y: 50, w: 20, h: 20 }
        const { cy, viewport } = viewportOver({ region: box })
        viewport.circuitRegionToggled('region')
        viewport.layoutSettled()
        expect(cy.pan()).toEqual(centeredOn(box))
        // Only the pan changes. The zoom stays where the user set it.
        expect(cy.zoom()).toBe(1)
    })

    it('gives an explicit request the last word over a toggle', () => {
        // Both are pending at the same time: a search expanded the ancestors of a node, and each
        // expansion is a toggle. The user asked for the node, so the view goes there.
        const asked = { x: 400, y: 400, w: 20, h: 20 }
        const { cy, viewport } = viewportOver({
            region: { x: 900, y: 900, w: 20, h: 20 },
            asked
        })
        viewport.circuitRegionToggled('region')
        viewport.centerOnNextLayout(Option.some('asked'))
        viewport.layoutSettled()
        expect(cy.pan()).toEqual(centeredOn(asked))
    })

    it('forgets the toggle after the layout it belongs to', () => {
        const box = { x: 111, y: 50, w: 20, h: 20 }
        const { cy, viewport } = viewportOver({ region: box })
        viewport.circuitRegionToggled('region')
        viewport.layoutSettled()

        // A later layout (for example, after a metric change or a resize) must not move the view back
        // to a node that the user has since panned away from.
        cy.pan({ x: 0, y: 0 })
        viewport.layoutSettled()
        expect(cy.pan()).toEqual({ x: 0, y: 0 })
    })

    it('ignores a node that is no longer drawn', () => {
        // Collapsing an ancestor can remove the toggled node from the graph. A missing element has no
        // position.
        const { cy, viewport } = viewportOver({})
        viewport.circuitRegionToggled('gone')
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
        // The focus zoom: `FOCUS_FONT_SIZE` (11.25) divided by `NODE_FONT_SIZE` (12). A search uses
        // the same zoom, so opening a profile and searching for a node in it give the same scale.
        const zoom = cy.zoom()
        expect(zoom).toBeCloseTo(11.25 / 12, 5)
        expect(cy.pan()).toEqual({ x: 50 - first.x * zoom, y: 50 - first.y * zoom })
    })

    it('leaves the view where it is on every layout after that', () => {
        const first = { x: 300, y: 300, w: 60, h: 20 }
        const { cy, viewport } = viewportOver({ n0: first }, 'n0')
        const placed = { ...cy.pan() }
        viewport.layoutSettled()
        expect(cy.pan()).toEqual(placed)
    })

    it('falls back to the whole graph when there is no node to open on', () => {
        // A circuit that has only its root node. The layout does not fit the graph in the view, so
        // without this call the view stays at its initial pan and zoom.
        const { cy } = viewportOver({}, undefined)
        expect(cy.fitCount()).toBe(1)
    })

    it('falls back to the whole graph when the node it is given is not drawn', () => {
        const { cy } = viewportOver({}, 'n0')
        expect(cy.fitCount()).toBe(1)
    })

    it('does not open further out than the whole circuit takes', () => {
        // The graph is 80x80 in a 100x100 view, so it fits at zoom 1.25. The zoom never goes below the
        // zoom that fits the graph, so a small circuit opens closer than the focus zoom.
        const { cy } = viewportOver({ n0: { x: 40, y: 40, w: 10, h: 10 } }, 'n0', { w: 80, h: 80 })
        expect(cy.zoom()).toBe(1.25)
    })
})

describe('a search', () => {
    it('zooms in until the node text is the size it is read at', () => {
        const { cy, viewport } = viewportOver({ n0: { x: 300, y: 300, w: 60, h: 8 } })
        cy.zoom(0.5)
        viewport.center('n0')
        // The focus zoom (11.25 / 12), which is also the zoom that a profile opens at. So a search
        // gives the same scale that the user saw when the profile opened.
        expect(cy.zoom()).toBeCloseTo(11.25 / 12, 5)
    })

    it('does not zoom out when the view is already closer than that', () => {
        const box = { x: 300, y: 300, w: 60, h: 20 }
        const { cy, viewport } = viewportOver({ n0: box })
        viewport.center('n0')
        expect(cy.zoom()).toBe(1)
        expect(cy.pan()).toEqual(centeredOn(box))
    })

    it('ignores an id that is not in the graph', () => {
        const { cy, viewport } = viewportOver({})
        expect(() => viewport.center('gone')).not.toThrow()
        expect(cy.pan()).toEqual({ x: 0, y: 0 })
    })
})

describe('the minimap', () => {
    it('redraws its picture once a layout has settled, and never while the view moves', () => {
        // Drawing the picture reads every element, so it happens only at the end of a layout, when the
        // elements stop moving. A pan or a zoom only moves the view outline on the picture.
        minimap.length = 0
        const { cy } = viewportOver({ n0: { x: 300, y: 300, w: 60, h: 20 } })
        expect(minimap.filter((call) => call === 'showGraph')).toHaveLength(1)

        for (const event of ['pan', 'zoom', 'resize', 'pan']) {
            cy.fire(event)
        }
        expect(minimap.filter((call) => call === 'showGraph')).toHaveLength(1)
        // One call from the layout, and one for each of the four events.
        expect(minimap.filter((call) => call === 'showView')).toHaveLength(5)
    })
})
