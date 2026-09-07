import cytoscape, { type Core } from 'cytoscape'
import { describe, expect, it } from 'vitest'
import { DIAGRAM_PALETTES } from './diagramTheme.js'
import {
    DRAW_EDGES,
    graphPicture,
    paintPicture,
    type GraphPicture,
    type PictureBox
} from './minimapPicture.js'
import { Point, Rectangle, Size } from './planar.js'

/** Geometry of the fixture below, kept apart from the diagram's own so the boxes come out exact. */
const NODE_WIDTH = 40
const NODE_HEIGHT = 20
const BORDER = 1
const PADDING = 10
/** How far apart the two operators inside the region sit. */
const NODE_GAP = 200

/** A region holding two operators, one operator beside it, an edge between the two, and the circuit's
 *  root node, which is on the graph but never drawn. */
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
            ],
            edges: [{ data: { id: 'e', source: 'plain', target: 'a' } }]
        }
    })
    cy.style([
        { selector: 'node', style: { width: NODE_WIDTH, height: NODE_HEIGHT, 'border-width': BORDER } },
        { selector: ':parent', style: { padding: PADDING } },
        { selector: 'node[invisible]', style: { display: 'none' } }
    ])
    // Positions are set after the fact: a headless instance puts every node it is handed on the origin.
    cy.$id('root').position({ x: 0, y: 0 })
    cy.$id('a').position({ x: 0, y: 0 })
    cy.$id('b').position({ x: 0, y: NODE_GAP })
    cy.$id('plain').position({ x: 300, y: 100 })
    return cy
}

/** The picture of `graph()`, which is never empty. */
const picture = (cy: Core = graph()): GraphPicture => graphPicture(cy)!

/** A box by its center and its size, which is how the diagram places a region. */
const centered = (x: number, y: number, w: number, h: number): PictureBox =>
    ({ x: x - w / 2, y: y - h / 2, w, h })

describe('graphPicture', () => {
    it('takes each operator as the point it sits on, in model coordinates', () => {
        // The same coordinates the graph is laid out in, so the map is one scale away from the diagram.
        // Its size is not read: every operator is drawn as the same dash.
        expect(picture().nodes).toEqual([{ x: 0, y: 0 }, { x: 0, y: NODE_GAP }, { x: 300, y: 100 }])
    })

    it('keeps the regions apart from the operators, so a region can be drawn behind them', () => {
        const { regions, nodes } = picture()
        expect(regions).toHaveLength(1)
        expect(nodes).toHaveLength(3)
        // The two operators it holds, plus the band its name is drawn in on each side, give or take the
        // borders cytoscape counts into a compound node's size. A region is the one thing on the map
        // whose size is its own.
        const spanned = { w: NODE_WIDTH + BORDER, h: NODE_GAP + NODE_HEIGHT + BORDER }
        const region = regions[0]!
        for (const side of [region.w - (spanned.w + 2 * PADDING), region.h - (spanned.h + 2 * PADDING)]) {
            expect(side).toBeGreaterThanOrEqual(0)
            expect(side).toBeLessThanOrEqual(4 * BORDER)
        }
    })

    it('leaves out a node the diagram does not draw', () => {
        // The circuit's root node is on the graph, hidden by the stylesheet. Of the four drawn, one is
        // the region.
        expect(picture().nodes).toHaveLength(3)
        expect(graph().nodes()).toHaveLength(5)
    })

    it('covers exactly the box the whole graph covers', () => {
        // The box the map is fitted to, so a picture that fills the map shows everything on the graph.
        const cy = graph()
        const box = cy.elements().boundingBox()
        expect(picture(cy).box).toEqual(new Rectangle(new Point(box.x1, box.y1), new Size(box.w, box.h)))
    })

    it('has nothing to show for a graph with nothing on it', () => {
        expect(graphPicture(cytoscape({ headless: true, styleEnabled: true }))).toBeNull()
    })

    it('collects an edge only while the map draws them, and then straight between the two nodes', () => {
        // Following `DRAW_EDGES`, because what this is here for is the other half: an edge left undrawn
        // is left uncollected too, so the flag saves the work and not only the ink.
        expect(picture().edges).toEqual(DRAW_EDGES ? [{ x1: 300, y1: 100, x2: 0, y2: 0 }] : [])
    })
})

/** The slice of `CanvasRenderingContext2D` the painter touches, recording what it is asked to draw. A
 *  fill or a stroke carries the style in effect when it was made. */
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
        // Here only so that a painter that draws text fails by name.
        fillText() { ops.push({ op: 'fillText' }) }
    }
    return context
}

/** Every op of painting `picture` at `scale`. */
const paint = (picture: GraphPicture, scale = 0.1, theme: 'light' | 'dark' = 'light'): Op[] => {
    const context = recorder()
    paintPicture(context as unknown as CanvasRenderingContext2D, picture, scale, theme)
    return context.ops
}

/** A picture holding `count` of everything, spread over a thousand model units. */
const sized = (count: number): GraphPicture => ({
    box: new Rectangle(Point.zero(), new Size(1000, 1000)),
    regions: Array.from({ length: count }, (_, i) => centered(i, i, 100, 100)),
    nodes: Array.from({ length: count }, (_, i) => ({ x: i, y: i })),
    edges: Array.from({ length: count }, (_, i) => ({ x1: i, y1: i, x2: i + 20, y2: i + 30 }))
})

/** The calls that rasterize; every other one only builds up a path. */
const rasterizing = (ops: Op[]): Op[] => ops.filter((op) => op.op === 'fill' || op.op === 'stroke')

describe('paintPicture', () => {
    it('rasterizes the whole picture in a fixed number of passes, however much is on it', () => {
        // What keeps a minimap of a large circuit off the frame budget: regions are one path, edges one,
        // operators one, so the rasterizing does not grow with the graph. Only the paths do.
        const few = paint(sized(2))
        const many = paint(sized(2000))
        expect(rasterizing(many)).toEqual(rasterizing(few))
        expect(rasterizing(many)).toHaveLength(4)
    })

    it('scales the model onto the map, and moves its corner to the origin', () => {
        const ops = paint(picture(), 0.25)
        expect(ops[0]).toEqual({ op: 'save' })
        expect(ops[1]).toEqual({ op: 'scale', args: [0.25, 0.25] })
        const box = picture().box.origin
        expect(ops[2]).toEqual({ op: 'translate', args: [-box.x, -box.y] })
        expect(ops[ops.length - 1]).toEqual({ op: 'restore' })
    })

    it('keeps every line a hairline on the map, whatever the map is scaled by', () => {
        // Drawn in model coordinates, where a width of one would be a line as thick as an operator.
        for (const scale of [0.01, 0.25, 1]) {
            for (const op of paint(sized(2), scale)) {
                if (op.op === 'stroke') {
                    expect(op.width! * scale, `${scale}`).toBeLessThanOrEqual(1)
                    expect(op.width! * scale, `${scale}`).toBeGreaterThan(0)
                }
            }
        }
    })

    it('draws every operator as the same dash, one pixel by desired width, at any scale', () => {
        // The dash is the whole of what an operator gets: a mark that says where it is, sized in map
        // pixels so a large circuit shows its dense places rather than fading out.
        for (const scale of [0.005, 0.25, 1]) {
            // Operators alone, so every line painted is one of theirs.
            const alone = { ...sized(1), regions: [], edges: [] }
            const ops = paint(alone, scale)
            const [from] = ops.filter((op) => op.op === 'moveTo').map((op) => op.args!)
            const [to] = ops.filter((op) => op.op === 'lineTo').map((op) => op.args!)
            const [line] = ops.filter((op) => op.op === 'stroke')
            expect((to![0]! - from![0]!) * scale, `${scale}`).toBeCloseTo(6, 6)
            expect(line!.width! * scale, `${scale}`).toBeCloseTo(1, 6)
            // Along the middle of the dash, so the operator's own point is the middle of the mark.
            expect(from![1]).toBe(to![1])
        }
    })

    it('draws the regions, then the edges, then the operators over both', () => {
        const styles = rasterizing(paint(sized(1))).map((op) => op.style)
        const palette = DIAGRAM_PALETTES.light
        // Operators and lines share one gray, which is not the black the viewport outline is drawn in.
        expect(styles).toEqual([
            palette.region,
            palette.border,
            palette.navigatorInk,
            palette.navigatorInk
        ])
        expect(palette.navigatorInk).not.toBe(palette.navigatorViewport)
    })

    it('tints a region the way the diagram tints it', () => {
        const [region] = rasterizing(paint(sized(1)))
        expect(region!.alpha).toBeLessThan(1)
        // And nothing else is translucent: the alpha is put back before the next pass.
        for (const op of rasterizing(paint(sized(1))).slice(2)) {
            expect(op.alpha ?? 1).toBe(1)
        }
    })

    it('paints in the palette it is given', () => {
        expect(rasterizing(paint(sized(1), 0.1, 'dark')).map((op) => op.style))
            .not.toEqual(rasterizing(paint(sized(1), 0.1, 'light')).map((op) => op.style))
    })

    it('paints no text: a node label at this size is a smudge', () => {
        expect(paint(picture()).map((op) => op.op)).not.toContain('fillText')
    })

    it('has nothing to draw for a picture with nothing on it', () => {
        const empty = { box: new Rectangle(Point.zero(), new Size(10, 10)), regions: [], nodes: [], edges: [] }
        expect(rasterizing(paint(empty))).toEqual([])
    })
})
