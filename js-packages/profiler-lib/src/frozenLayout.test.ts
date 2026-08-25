// What is on screen between a graph change and the layout that follows it. The states are a handful of
// DOM moves over cytoscape's canvases, so the canvases are stood in for here and asked what was done to
// them. That the copy looks like the diagram needs a renderer, and belongs to profiler-layout's browser
// suite; that the copy goes up, stays up and comes down again is this file's.

import { describe, expect, it } from 'vitest'

import type { Core } from 'cytoscape'
import { FrozenLayout } from './frozenLayout.js'

/** Device pixels of cytoscape's layers, which the copy has to match to be as sharp as they are. */
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

/** Cytoscape's three canvases inside the wrapper it positions them in, and enough of a document to make
 *  one more. `blits` records what was drawn into the copy, in order. */
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
        // The copy is a canvas too, and lands in the same container, so a query after it goes up finds it.
        querySelectorAll: () => [...canvases, ...overlays]
    }

    const cy = { container: () => (headless ? null : container) } as unknown as Core
    return { cy, container, canvases, overlays, blits }
}

/** A diagram past its first layout, which is the point at which there is something to hold over. */
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
        // Every layer, bottom to top, into a copy the size of the layers themselves.
        expect(blits).toEqual(['background', 'nodes', 'drag'])
        expect(overlays[0]!.width).toBe(LAYER_WIDTH)
        expect(overlays[0]!.height).toBe(LAYER_HEIGHT)
        expect(hidden(canvases)).toEqual(['hidden', 'hidden', 'hidden'])
        // The copy is the one canvas that has to stay visible, though it is in the container too.
        expect(overlays[0]!.style.visibility).toBe('')
    })

    it('keeps the copy it already has when the graph changes again before it settles', () => {
        const { frozen, overlays, blits } = shownOnce()
        frozen.graphWillChange()
        frozen.graphWillChange()

        // A second copy would be of the canvases hidden behind the first one, which hold the layout the
        // user never saw finish.
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

        // Nothing else is coming to do it: without this the user is left looking at an image that
        // answers no clicks, the live canvases being hidden behind it.
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
