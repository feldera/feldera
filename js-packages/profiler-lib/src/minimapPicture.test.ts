import cytoscape, { type Core } from 'cytoscape'
import { describe, expect, it } from 'vitest'
import { DIAGRAM_PALETTES } from './diagramTheme.js'
import { graphPicture, paintPicture, type GraphPicture } from './minimapPicture.js'
import { Point, Rectangle, Size } from './planar.js'

/** Sizes for the fixture below. The fixture does not use the diagram styles, so the expected boxes are
 *  exact numbers. */
const NODE_WIDTH = 40
const NODE_HEIGHT = 20
const BORDER = 1
const PADDING = 10
/** The vertical distance between the two operators in the region. */
const NODE_GAP = 200

/** A region with two operators, one operator outside the region, and the root node of the circuit,
 *  which is in the graph but hidden. */
function graph(): Core {
    const cy = cytoscape({
        headless: true,
        styleEnabled: true,
        elements: {
            nodes: [
                { data: { id: 'root', invisible: true } },
                { data: { id: 'region' } },
                { data: { id: 'a', parent: 'region' } },
                { data: { id: 'b', parent: 'region' } },
                { data: { id: 'plain' } }
            ]
        }
    })
    cy.style([
        { selector: 'node', style: { width: NODE_WIDTH, height: NODE_HEIGHT, 'border-width': BORDER } },
        { selector: ':parent', style: { padding: PADDING } },
        { selector: 'node[invisible]', style: { display: 'none' } }
    ])
    // Set the positions after creation, because a headless instance puts every node at the origin.
    cy.$id('root').position({ x: 0, y: 0 })
    cy.$id('a').position({ x: 0, y: 0 })
    cy.$id('b').position({ x: 0, y: NODE_GAP })
    cy.$id('plain').position({ x: 300, y: 100 })
    return cy
}

/** The picture of `graph()`, which is never empty. */
const picture = (cy: Core = graph()): GraphPicture => graphPicture(cy)!

describe('graphPicture', () => {
    it('returns the center of each operator, in model coordinates', () => {
        // The points are in the coordinates of the diagram, so the minimap differs from the diagram
        // only by the scale. The node size is not read, because every operator is the same dash.
        expect(picture().nodes).toEqual([new Point(0, 0), new Point(0, NODE_GAP), new Point(300, 100)])
    })

    it('returns the regions separately from the operators, with their full size', () => {
        const { regions, nodes } = picture()
        expect(regions).toHaveLength(1)
        expect(nodes).toHaveLength(3)
        // The region contains the two operators, with `PADDING` on each side. Cytoscape also adds some
        // border widths to the size of a compound node, so allow up to `4 * BORDER` more.
        const spanned = { w: NODE_WIDTH + BORDER, h: NODE_GAP + NODE_HEIGHT + BORDER }
        const region = regions[0]!
        const { w, h } = region.size
        for (const side of [w - (spanned.w + 2 * PADDING), h - (spanned.h + 2 * PADDING)]) {
            expect(side).toBeGreaterThanOrEqual(0)
            expect(side).toBeLessThanOrEqual(4 * BORDER)
        }
    })

    it('skips hidden nodes', () => {
        // The stylesheet hides the root node of the circuit. One of the four visible nodes is the
        // region.
        expect(picture().nodes).toHaveLength(3)
        expect(graph().nodes()).toHaveLength(5)
    })

    it('uses the bounding box of the whole graph', () => {
        // The minimap is fitted to this box, so the picture shows everything in the graph.
        const cy = graph()
        const box = cy.elements().boundingBox()
        expect(picture(cy).box).toEqual(new Rectangle(new Point(box.x1, box.y1), new Size(box.w, box.h)))
    })

    it('returns null for an empty graph', () => {
        expect(graphPicture(cytoscape({ headless: true, styleEnabled: true }))).toBeNull()
    })
})

/** A mock of the part of `CanvasRenderingContext2D` that `paintPicture` uses. It records each call. The
 *  record of a `fill` or a `stroke` also has the style that was set at that time. */
interface Op { op: string, args?: number[], style?: string, width?: number, alpha?: number }
const recorder = () => {
    const ops: Op[] = []
    const context = {
        ops,
        fillStyle: '',
        strokeStyle: '',
        lineWidth: 0,
        globalAlpha: 1,
        save() { ops.push({ op: 'save' }) },
        restore() { ops.push({ op: 'restore' }) },
        scale(x: number, y: number) { ops.push({ op: 'scale', args: [x, y] }) },
        translate(x: number, y: number) { ops.push({ op: 'translate', args: [x, y] }) },
        beginPath() { ops.push({ op: 'beginPath' }) },
        rect(...args: number[]) { ops.push({ op: 'rect', args }) },
        moveTo(...args: number[]) { ops.push({ op: 'moveTo', args }) },
        lineTo(...args: number[]) { ops.push({ op: 'lineTo', args }) },
        fill() { ops.push({ op: 'fill', style: this.fillStyle, alpha: this.globalAlpha }) },
        stroke() { ops.push({ op: 'stroke', style: this.strokeStyle, width: this.lineWidth }) },
        // If `paintPicture` draws text, this records it, so the test fails with a clear message
        // instead of a `TypeError`.
        fillText() { ops.push({ op: 'fillText' }) }
    }
    return context
}

/** All recorded calls when `picture` is painted at `scale`. */
const paint = (picture: GraphPicture, scale = 0.1, theme: 'light' | 'dark' = 'light'): Op[] => {
    const context = recorder()
    paintPicture(context as unknown as CanvasRenderingContext2D, picture, scale, theme)
    return context.ops
}

/** A picture with `count` regions and `count` operators, in a box of 1000 by 1000 model units. */
const sized = (count: number): GraphPicture => ({
    box: new Rectangle(Point.zero(), new Size(1000, 1000)),
    regions: Array.from({ length: count }, (_, i) =>
        Rectangle.centered(new Point(i, i), new Size(100, 100))),
    nodes: Array.from({ length: count }, (_, i) => new Point(i, i))
})

/** Only the calls that draw pixels (`fill` and `stroke`). The other calls only add shapes to a path. */
const rasterizing = (ops: Op[]): Op[] => ops.filter((op) => op.op === 'fill' || op.op === 'stroke')

describe('paintPicture', () => {
    it('uses three draw calls for any number of shapes', () => {
        // There is one path for the regions and one for the operators. So the number of draw calls does
        // not change with the size of the circuit. Only the number of shapes does.
        const few = paint(sized(2))
        const many = paint(sized(2000))
        expect(rasterizing(many)).toEqual(rasterizing(few))
        expect(rasterizing(many)).toHaveLength(3)
    })

    it('scales model units to minimap pixels, and moves the box corner to the origin', () => {
        const ops = paint(picture(), 0.25)
        expect(ops[0]).toEqual({ op: 'save' })
        expect(ops[1]).toEqual({ op: 'scale', args: [0.25, 0.25] })
        const box = picture().box.origin
        expect(ops[2]).toEqual({ op: 'translate', args: [-box.x, -box.y] })
        expect(ops[ops.length - 1]).toEqual({ op: 'restore' })
    })

    it('draws lines at most 1 minimap pixel wide, at any scale', () => {
        // `paintPicture` draws in model coordinates. There, a width of 1 is 1 model unit, so the line
        // width on screen would change with the scale.
        for (const scale of [0.01, 0.25, 1]) {
            for (const op of paint(sized(2), scale)) {
                if (op.op === 'stroke') {
                    expect(op.width! * scale, `${scale}`).toBeLessThanOrEqual(1)
                    expect(op.width! * scale, `${scale}`).toBeGreaterThan(0)
                }
            }
        }
    })

    it('draws each operator as a dash of 6 by 1 minimap pixels, at any scale', () => {
        // An operator is drawn only as a dash. The dash size is in minimap pixels, so on a large
        // circuit the dashes stay visible and show where the operators are dense.
        for (const scale of [0.005, 0.25, 1]) {
            // Only operators, so each line in the output is an operator dash.
            const alone = { ...sized(1), regions: [] }
            const ops = paint(alone, scale)
            const [from] = ops.filter((op) => op.op === 'moveTo').map((op) => op.args!)
            const [to] = ops.filter((op) => op.op === 'lineTo').map((op) => op.args!)
            const [line] = ops.filter((op) => op.op === 'stroke')
            expect((to![0]! - from![0]!) * scale, `${scale}`).toBeCloseTo(6, 6)
            expect(line!.width! * scale, `${scale}`).toBeCloseTo(1, 6)
            // The dash is horizontal.
            expect(from![1]).toBe(to![1])
        }
    })

    it('draws the regions, then the operators', () => {
        const styles = rasterizing(paint(sized(1))).map((op) => op.style)
        const palette = DIAGRAM_PALETTES.light
        // Operators are gray (`navigatorInk`). It is not the color of the viewport outline.
        expect(styles).toEqual([palette.region, palette.border, palette.navigatorInk])
        expect(palette.navigatorInk).not.toBe(palette.navigatorViewport)
    })

    it('fills the regions with a translucent color, as the diagram does', () => {
        const [region] = rasterizing(paint(sized(1)))
        expect(region!.alpha).toBeLessThan(1)
        // No other draw call is translucent: `paintPicture` sets the alpha back to 1 after the region
        // fill.
        for (const op of rasterizing(paint(sized(1))).slice(2)) {
            expect(op.alpha ?? 1).toBe(1)
        }
    })

    it('uses the colors of the given theme', () => {
        expect(rasterizing(paint(sized(1), 0.1, 'dark')).map((op) => op.style))
            .not.toEqual(rasterizing(paint(sized(1), 0.1, 'light')).map((op) => op.style))
    })

    it('draws no text', () => {
        expect(paint(picture()).map((op) => op.op)).not.toContain('fillText')
    })

    it('draws nothing for an empty picture', () => {
        const empty = { box: new Rectangle(Point.zero(), new Size(10, 10)), regions: [], nodes: [] }
        expect(rasterizing(paint(empty))).toEqual([])
    })
})
