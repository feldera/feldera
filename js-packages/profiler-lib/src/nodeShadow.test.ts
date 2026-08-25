import cytoscape from 'cytoscape'
import { describe, expect, it } from 'vitest'
import {
    installNodeShadows,
    nodeShadow,
    SELECTED_NODE_CLASS,
    SELECTION_GLOW,
    shadowReach
} from './nodeShadow.js'

const graph = () =>
    cytoscape({
        headless: true,
        styleEnabled: true,
        elements: {
            nodes: [
                { data: { id: 'plain' } },
                // A composite that is still collapsed: it has children, but none of them are on the
                // graph, so cytoscape does not see it as a parent.
                { data: { id: 'collapsed', has_children: true } },
                { data: { id: 'region', has_children: true } },
                { data: { id: 'inside', parent: 'region' } }
            ],
            edges: []
        }
    })

/** A node of `graph()`, marked as the one the diagram reports on. */
const selected = (id: string) => {
    const node = graph().$id(id)
    node.addClass(SELECTED_NODE_CLASS)
    return node
}

describe('nodeShadow', () => {
    it('glows around the node the diagram reports on', () => {
        for (const id of ['plain', 'collapsed', 'inside']) {
            expect(nodeShadow(selected(id)), id).toBe(SELECTION_GLOW)
        }
    })

    it('leaves every other node bare', () => {
        // Nothing is painted around an unmarked node: its own border is what tells it from the canvas.
        const cy = graph()
        for (const id of ['plain', 'collapsed', 'inside']) {
            expect(nodeShadow(cy.$id(id)), id).toBeNull()
        }
    })

    it('leaves an expanded region flat, marked or not', () => {
        // The glow would run along the inside of the region's border and read as a border of its own.
        const cy = graph()
        expect(cy.$id('region').isParent()).toBe(true)
        expect(nodeShadow(cy.$id('region'))).toBeNull()
        cy.$id('region').addClass(SELECTED_NODE_CLASS)
        expect(nodeShadow(cy.$id('region'))).toBeNull()
    })

    it('centers the glow and spreads it wide', () => {
        // The glow marks one node out of hundreds, so it has to carry past the node's own edge;
        // centering is what makes it read as the node being lit.
        expect(shadowReach(SELECTION_GLOW)).toBeGreaterThan(0)
        expect(SELECTION_GLOW.blur).toBeGreaterThan(0)
        expect(SELECTION_GLOW.offsetX).toBe(0)
        expect(SELECTION_GLOW.offsetY).toBe(0)
    })
})

describe('installNodeShadows', () => {
    it('wraps the renderer hook, passing every argument through', () => {
        // The hook is not part of cytoscape's documented API, so this pins the shape it is called
        // with: a context and a node first, the node's precomputed geometry after.
        const calls: unknown[][] = []
        const renderer = { drawNodeUnderlay: (...args: unknown[]) => calls.push(args) }
        const cy = { renderer: () => renderer } as never
        installNodeShadows(cy)
        expect(renderer.drawNodeUnderlay).not.toBe(undefined)

        const context = fakeContext()
        const node = selected('plain')
        const pos = { x: 10, y: 20 }
        renderer.drawNodeUnderlay(context as never, node as never, pos as never, 40 as never, 25 as never)
        expect(calls).toEqual([[context, node, pos, 40, 25]])
        // And it painted the glow: a single filled round rectangle, moved out of frame so that only
        // its shadow lands on the canvas.
        expect(context.log.filter((op) => op === 'fill')).toHaveLength(1)
        expect(context.shadowColor).toBe(SELECTION_GLOW.color)
        expect(context.roundRects).toHaveLength(1)
        expect(context.roundRects[0]!.x).toBeLessThan(-1000)
        expect(context.shadowOffsetX).toBeGreaterThan(1000)
    })

    it('paints nothing around an unmarked node', () => {
        // Not a pass that paints a color the canvas quietly drops: the painter is never entered, so
        // every node but one does no per-node work at all.
        const renderer = { drawNodeUnderlay: () => {} }
        installNodeShadows({ renderer: () => renderer } as never)
        const context = fakeContext()
        renderer.drawNodeUnderlay(context as never, graph().$id('plain') as never)
        expect(context.log).toEqual([])
        expect(context.roundRects).toEqual([])
        expect(context.shadowColor).toBe('')
    })

    it('leaves a headless instance alone', () => {
        // No canvas renderer, so nothing to paint on; the call must not throw on the way through.
        const cy = { renderer: () => ({}) } as never
        expect(() => installNodeShadows(cy)).not.toThrow()
    })

    it('scales blur and offset by the transform in effect', () => {
        // Canvas shadows are measured in device pixels, so a glow left unscaled would keep its size
        // on screen while the node it surrounds grows and shrinks with the zoom.
        const scaled = (scale: number) => {
            const renderer = { drawNodeUnderlay: () => {} }
            installNodeShadows({ renderer: () => renderer } as never)
            const context = fakeContext(scale)
            renderer.drawNodeUnderlay(context as never, selected('plain') as never)
            return context
        }
        const one = scaled(1)
        const four = scaled(4)
        expect(four.shadowBlur).toBeCloseTo(one.shadowBlur * 4, 5)
        expect(four.shadowOffsetX).toBeCloseTo(one.shadowOffsetX * 4, 5)
    })
})

/** The slice of `CanvasRenderingContext2D` the painter touches, recording what it is asked to do. */
const fakeContext = (scale = 1) => ({
    log: [] as string[],
    roundRects: [] as Array<{ x: number, y: number, w: number, h: number }>,
    fillStyle: '',
    shadowColor: '',
    shadowBlur: 0,
    shadowOffsetX: 0,
    shadowOffsetY: 0,
    getTransform: () => ({ a: scale }),
    save() { this.log.push('save') },
    restore() { this.log.push('restore') },
    beginPath() { this.log.push('beginPath') },
    roundRect(x: number, y: number, w: number, h: number) { this.roundRects.push({ x, y, w, h }) },
    fill() { this.log.push('fill') }
})
