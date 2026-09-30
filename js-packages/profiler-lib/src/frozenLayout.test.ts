// Unit tests for `FrozenLayout`, which shows a still copy of the diagram while the graph changes and
// a new layout is calculated. `FrozenLayout` only adds, removes, hides and shows canvas elements. So
// these tests give it fake canvases, and then check what it did to them. These tests check when the
// copy is added and removed. The browser tests in profiler-layout check that the copy looks the same
// as the diagram, because that needs a real renderer.

import { describe, expect, it } from 'vitest'

import type { Core } from 'cytoscape'
import { FrozenLayout } from './frozenLayout.js'

/** The size of cytoscape's canvases in device pixels. The copy must have the same size, so that it is
 *  as sharp as the canvases. */
const LAYER_WIDTH = 1600
const LAYER_HEIGHT = 1200

interface FakeCanvas {
    name: string
    width: number
    height: number
    style: { cssText: string, visibility: string }
    getContext(kind: string): { drawImage(layer: FakeCanvas): void } | null
    remove(): void
}

/** A fake container with cytoscape's three canvases in a wrapper element, as cytoscape makes them. Its
 *  `ownerDocument` can make one more canvas, for the copy. `blits` records the names of the canvases
 *  that were drawn into the copy, in order. */
function fakeDiagram({ headless = false } = {}) {
    const blits: string[] = []
    const overlays: FakeCanvas[] = []

    const canvas = (name: string): FakeCanvas => ({
        name,
        width: LAYER_WIDTH,
        height: LAYER_HEIGHT,
        style: { cssText: '', visibility: '' },
        getContext: (kind) =>
            kind === '2d' ? { drawImage: (layer: FakeCanvas) => blits.push(layer.name) } : null,
        remove() {
            const at = overlays.indexOf(this)
            if (at >= 0) {
                overlays.splice(at, 1)
            }
        }
    })

    const layers = { appendChild: (child: FakeCanvas) => overlays.push(child) }
    const canvases = ['background', 'nodes', 'drag'].map(canvas)
    const container = {
        style: { visibility: '' },
        firstElementChild: layers,
        ownerDocument: { createElement: () => canvas('copy') },
        // The copy is also a canvas in the container. So after the copy is added, a query for canvases
        // finds it too.
        querySelectorAll: () => [...canvases, ...overlays]
    }

    const cy = { container: () => (headless ? null : container) } as unknown as Core
    return { cy, container, canvases, overlays, blits }
}

/** A diagram that has finished its first layout. Before that, there is no picture to copy. */
function shownOnce(options?: { headless: boolean }) {
    const diagram = fakeDiagram(options)
    const frozen = new FrozenLayout(diagram.cy)
    frozen.graphWillChange()
    frozen.layoutSettled()
    return { ...diagram, frozen }
}

const hidden = (canvases: FakeCanvas[]) => canvases.map((canvas) => canvas.style.visibility)

describe('the first layout', () => {
    it('hides the container, having no earlier layout to hold over', () => {
        const { cy, container, overlays } = fakeDiagram()
        new FrozenLayout(cy).graphWillChange()
        expect(container.style.visibility).toBe('hidden')
        expect(overlays).toHaveLength(0)
    })

    it('shows the container once it has settled', () => {
        const { container } = shownOnce()
        expect(container.style.visibility).toBe('')
    })
})

describe('a layout after that', () => {
    it('copies the canvases into an overlay and hides them behind it', () => {
        const { frozen, canvases, overlays, blits } = shownOnce()
        frozen.graphWillChange()

        expect(overlays).toHaveLength(1)
        // All three canvases are drawn into the copy, from bottom to top. The copy has the same size as
        // the canvases.
        expect(blits).toEqual(['background', 'nodes', 'drag'])
        expect(overlays[0]!.width).toBe(LAYER_WIDTH)
        expect(overlays[0]!.height).toBe(LAYER_HEIGHT)
        expect(hidden(canvases)).toEqual(['hidden', 'hidden', 'hidden'])
        // The copy is a canvas in the same container, but it is the one canvas that must stay visible.
        expect(overlays[0]!.style.visibility).toBe('')
    })

    it('keeps the copy it already has when the graph changes again before it settles', () => {
        const { frozen, overlays, blits } = shownOnce()
        frozen.graphWillChange()
        frozen.graphWillChange()

        // A second copy would show the hidden canvases behind the first copy, and those show a layout
        // that is not finished. The first copy shows the last layout that the user saw.
        expect(overlays).toHaveLength(1)
        expect(blits).toEqual(['background', 'nodes', 'drag'])
    })

    it('takes the copy down when the layout settles', () => {
        const { frozen, canvases, overlays } = shownOnce()
        frozen.graphWillChange()
        frozen.layoutSettled()

        expect(overlays).toHaveLength(0)
        expect(hidden(canvases)).toEqual(['', '', ''])
    })

    it('takes the copy down when the layout never starts', () => {
        const { frozen, canvases, overlays } = shownOnce()
        frozen.graphWillChange()
        frozen.layoutFailed()

        // No `layoutSettled` call will come to remove the copy. Without this call, the user sees an
        // image that ignores clicks, and the real canvases stay hidden behind it.
        expect(overlays).toHaveLength(0)
        expect(hidden(canvases)).toEqual(['', '', ''])
    })

    it('takes the copy down when the diagram goes away', () => {
        const { frozen, canvases, overlays } = shownOnce()
        frozen.graphWillChange()
        frozen.dispose()

        expect(overlays).toHaveLength(0)
        expect(hidden(canvases)).toEqual(['', '', ''])
    })
})

describe('a headless diagram', () => {
    it('has nothing to freeze and does not try', () => {
        const { frozen, overlays, blits } = shownOnce({ headless: true })
        expect(() => {
            frozen.graphWillChange()
            frozen.layoutSettled()
            frozen.dispose()
        }).not.toThrow()
        expect(overlays).toHaveLength(0)
        expect(blits).toEqual([])
    })
})
