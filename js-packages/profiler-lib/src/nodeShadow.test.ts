import cytoscape from 'cytoscape'
import { describe, expect, it } from 'vitest'
import { DIAGRAM_PALETTES, NO_SHADOW } from './diagramTheme.js'
import {
    AMBIENT_SHADOW,
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

/** The palettes whose nodes cast an ambient shadow at all. */
const CASTING = (['light', 'dark'] as const).filter((theme) => AMBIENT_SHADOW[theme] !== null)

describe('nodeShadow', () => {
    it('casts every ambient shadow it has down and to the right', () => {
        expect(CASTING).not.toHaveLength(0)
        for (const theme of CASTING) {
            const shadow = AMBIENT_SHADOW[theme]!
            // The whole diagram is lit from one direction, so every node's shadow falls the same way.
            expect(shadow.offsetX, theme).toBeGreaterThan(0)
            expect(shadow.offsetY, theme).toBeGreaterThan(0)
            // Blurred, or it reads as a second border rather than as a shadow.
            expect(shadow.blur, theme).toBeGreaterThan(0)
        }
    })

    it('casts no ambient shadow at all in the dark palette', () => {
        // Deliberate, not an omission: a dark node is told from the canvas by its own border, which a
        // shadow behind it muddies. `NO_SHADOW` is how a palette says so, and it has to come out as a
        // node that casts nothing; `installNodeShadows` below covers the other half, that nothing is
        // painted either.
        const cy = graph()
        expect(DIAGRAM_PALETTES.dark.shadow).toBe(NO_SHADOW)
        expect(AMBIENT_SHADOW.dark).toBeNull()
        for (const id of ['plain', 'collapsed', 'inside']) {
            expect(nodeShadow(cy.$id(id), 'dark'), id).toBeNull()
        }
        // The light palette still casts one, so this is the palette's decision and not a switch
        // turned off for the whole diagram.
        expect(nodeShadow(cy.$id('plain'), 'light')).toBe(AMBIENT_SHADOW.light)
    })

    it('marks the selected node in the dark palette too, where nothing else is shadowed', () => {
        // The glow is not the palette's to decline: it is the one node the diagram is reporting on,
        // and declining the ambient shadow is exactly what leaves the glow alone on the canvas.
        const cy = graph()
        const node = cy.$id('plain')
        node.addClass(SELECTED_NODE_CLASS)
        expect(nodeShadow(node, 'dark')).toBe(SELECTION_GLOW)
    })

    it('keeps the accent glow the same in either palette', () => {
        const cy = graph()
        cy.$id('plain').addClass(SELECTED_NODE_CLASS)
        expect(nodeShadow(cy.$id('plain'), 'dark')).toBe(nodeShadow(cy.$id('plain'), 'light'))
    })

    it('gives every non-expanded node the ambient shadow', () => {
        const cy = graph()
        for (const id of ['plain', 'collapsed', 'inside']) {
            expect(nodeShadow(cy.$id(id), 'light'), id).toBe(AMBIENT_SHADOW.light)
        }
    })

    it('leaves an expanded region flat, selected or not', () => {
        // A region is a container drawn around nodes that cast their own shadows; giving it one too
        // would double up along every inner edge of the region.
        const cy = graph()
        expect(cy.$id('region').isParent()).toBe(true)
        expect(nodeShadow(cy.$id('region'), 'light')).toBeNull()
        cy.$id('region').addClass(SELECTED_NODE_CLASS)
        expect(nodeShadow(cy.$id('region'), 'light')).toBeNull()
    })

    it('replaces the shadow of the selected node with the accent glow', () => {
        const cy = graph()
        for (const id of ['plain', 'collapsed']) {
            const node = cy.$id(id)
            node.addClass(SELECTED_NODE_CLASS)
            expect(nodeShadow(node, 'light'), id).toBe(SELECTION_GLOW)
            node.removeClass(SELECTED_NODE_CLASS)
            expect(nodeShadow(node, 'light'), id).toBe(AMBIENT_SHADOW.light)
        }
    })

    it('spreads the glow wider than the ambient shadow, and centers it', () => {
        // The glow marks one node out of hundreds, so it has to carry further than the shadow every
        // other node casts; centering is what makes it read as the node being lit.
        expect(shadowReach(SELECTION_GLOW)).toBeGreaterThan(shadowReach(AMBIENT_SHADOW.light!))
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
        installNodeShadows(cy, () => 'light')
        expect(renderer.drawNodeUnderlay).not.toBe(undefined)

        const context = fakeContext()
        const node = graph().$id('plain')
        const pos = { x: 10, y: 20 }
        renderer.drawNodeUnderlay(context as never, node as never, pos as never, 40 as never, 25 as never)
        expect(calls).toEqual([[context, node, pos, 40, 25]])
        // And it painted the shadow: a single filled round rectangle, moved out of frame so that
        // only its shadow lands on the canvas.
        expect(context.log.filter((op) => op === 'fill')).toHaveLength(1)
        expect(context.shadowColor).toBe(AMBIENT_SHADOW.light!.color)
        expect(context.roundRects).toHaveLength(1)
        expect(context.roundRects[0]!.x).toBeLessThan(-1000)
        expect(context.shadowOffsetX).toBeGreaterThan(1000)
    })

    it('paints nothing where the palette casts no ambient shadow', () => {
        // Not a pass that paints a color the canvas quietly drops: no shadow means the painter is
        // never entered, so a dark diagram does no per-node work for a shadow it does not have.
        const renderer = { drawNodeUnderlay: () => {} }
        installNodeShadows({ renderer: () => renderer } as never, () => 'dark')
        const context = fakeContext()
        renderer.drawNodeUnderlay(context as never, graph().$id('plain') as never)
        expect(context.log).toEqual([])
        expect(context.roundRects).toEqual([])
        expect(context.shadowColor).toBe('')
    })

    it('leaves a headless instance alone', () => {
        // No canvas renderer, so nothing to paint on; the call must not throw on the way through.
        const cy = { renderer: () => ({}) } as never
        expect(() => installNodeShadows(cy, () => 'light')).not.toThrow()
    })

    it('scales blur and offset by the transform in effect', () => {
        // Canvas shadows are measured in device pixels, so a shadow left unscaled would keep its
        // size on screen while the node it belongs to grows and shrinks with the zoom.
        const scaled = (scale: number) => {
            const renderer = { drawNodeUnderlay: () => {} }
            installNodeShadows({ renderer: () => renderer } as never, () => 'light')
            const context = fakeContext(scale)
            renderer.drawNodeUnderlay(context as never, graph().$id('plain') as never)
            return context
        }
        const one = scaled(1)
        const four = scaled(4)
        expect(four.shadowBlur).toBeCloseTo(one.shadowBlur * 4, 5)
        expect(four.shadowOffsetY).toBeCloseTo(one.shadowOffsetY * 4, 5)
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
