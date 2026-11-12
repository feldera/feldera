// The stylesheet is exercised through a headless cytoscape instance, which resolves styles
// without a renderer. That is what pins the two mechanisms the diagram depends on and that fail
// silently otherwise: the per-node chip image list, and taxi edge routing.

import cytoscape from 'cytoscape'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
    badgePillWidth,
    BADGE_HEIGHT,
    CHIP_FONT_SIZE,
    CHIP_INSET,
    CHIP_NONE,
    CODE_CHIP_HEIGHT,
    formatLeafCount,
    nodeChips
} from './chips.js'
import {
    buildGraphStyle,
    COLLAPSED_REGION_CORNER_RADIUS,
    COLLAPSED_REGION_OUTER_HEIGHT,
    DIAGRAM_PALETTES,
    type DiagramTheme,
    ID_FONT_WEIGHT,
    labelWidth,
    NODE_CORNER_RADIUS,
    NODE_OUTER_HEIGHT,
    NODE_PADDING_X,
    RAISED_EDGE_CLASS,
    REGION_CORNER_RADIUS,
    REGION_OPACITY,
    REGION_PADDING
} from './diagramTheme.js'
import { REGION_CHIP_ROW_ROOM, regionMinWidth, reserveRegionChipRows } from './regionSize.js'

const graph = (theme: DiagramTheme) =>
    cytoscape({
        headless: true,
        styleEnabled: true,
        style: buildGraphStyle(theme),
        elements: {
            nodes: [
                { data: { id: 'plain', label: 'plain map', operator: 'map', chips: nodeChips(false, 0, theme) } },
                { data: { id: 'cool', label: 'cool', value: 5, chips: nodeChips(false, 0, theme) } },
                { data: { id: 'hot', label: 'hot', value: 99, chips: nodeChips(false, 0, theme) } },
                { data: { id: 'both', label: 'both', has_source: true, chips: nodeChips(true, 9, theme) } },
                // A circuit region that is still collapsed: children exist but are not on the graph.
                {
                    data: {
                        id: 'collapsed',
                        label: 'collapsed shard',
                        operator: 'shard',
                        has_children: true,
                        chips: nodeChips(false, 7, theme)
                    }
                },
                // An expanded region and the operator inside it. Note that no `depth` is set:
                // region styling must not depend on data written by a later graph update.
                { data: { id: 'region', label: 'region', has_children: true, chips: nodeChips(false, 4, theme) } },
                { data: { id: 'inside', label: 'inside', parent: 'region', chips: nodeChips(false, 0, theme) } },
                { data: { id: 'inside2', label: 'inside2', parent: 'region', chips: nodeChips(false, 0, theme) } }
            ],
            edges: [
                { data: { id: 'e', source: 'plain', target: 'both' } },
                // Wholly inside the region: the edge that would otherwise draw over the region's chips.
                { data: { id: 'inner', source: 'inside', target: 'inside2' } }
            ]
        }
    })

/** Style properties whose values are per-chip-slot lists. Every one of them has to stay aligned
 *  with the two image slots, or a chip lands in the wrong place / at the wrong size. */
const SLOT_PROPERTIES = [
    'background-image-containment',
    'background-clip',
    'background-repeat',
    'background-width',
    'background-height',
    'background-position-x',
    'background-position-y',
    'background-offset-x',
    'background-offset-y'
]

describe('chip styling', () => {
    it('resolves both chip images per node', () => {
        // A `data()` mapper is legal for the whole multi-valued property but not for a single
        // entry of a literal list, so the image list has to come from node data as one array.
        const cy = graph('light')
        const [code, badge] = nodeChips(true, 9, 'light')
        const resolved = cy.$id('both').style('background-image')
        expect(resolved).toContain(code)
        expect(resolved).toContain(badge)
        // An empty slot must still occupy its index, so slot 1 keeps its own geometry.
        expect(cy.$id('plain').style('background-image')).toBe(`${CHIP_NONE} ${CHIP_NONE}`)
    })

    it('anchors both slots to the top right corner', () => {
        // Stacked there, and both measured from the same two edges, which is what makes the offsets
        // below comparable with each other.
        const node = graph('light').$id('both')
        expect(node.style('background-position-x')).toBe('100% 100%')
        expect(node.style('background-position-y')).toBe('0% 0%')
    })

    it('keeps every per-slot property list aligned with the two image slots', () => {
        const cy = graph('light')
        const node = cy.$id('both')
        for (const property of SLOT_PROPERTIES) {
            expect(node.style(property).split(' '), property).toHaveLength(2)
        }
    })

    it('expands the node bounds over the code chip alone, the one part drawn outside the box', () => {
        // The bounding box drives the render cache, so a side left uncovered clips what is drawn
        // there - and an over-covered side is dead space the ELK layout spaces nodes by.
        const node = graph('light').$id('both')
        const top = node.position().y - node.outerHeight() / 2
        const bottom = node.position().y + node.outerHeight() / 2
        const box = node.boundingBox()
        expect(top - box.y1).toBeGreaterThanOrEqual(CODE_CHIP_HEIGHT)
        // Cytoscape pads every bounding box by a pixel of its own against antialiasing.
        expect(box.y2 - bottom).toBeLessThanOrEqual(1)
    })

    it('rests the code chip on the top edge and puts the counter just inside it', () => {
        const cy = graph('light')
        // The same stack on every node kind: one rule places both chips, so no override can move a
        // window control away from where the pointer learned to find it.
        for (const id of ['both', 'collapsed', 'region']) {
            const [code, counter] = cy.$id(id).style('background-offset-y').split(' ')
                .map((offset) => Number.parseFloat(offset!))
            // The code chip hangs its whole height above the edge, so its bottom rests on it.
            expect(code, id).toBe(-CODE_CHIP_HEIGHT)
            // The counter starts just below that same edge, inside the node.
            expect(counter, id).toBe(CHIP_INSET)
            // Which leaves exactly the gap between the two that the inset is.
            expect(counter! - (code! + CODE_CHIP_HEIGHT), id).toBe(CHIP_INSET)
        }
    })

    it('keeps both chips within the node horizontally', () => {
        // Both are anchored by their right edge, just inside the node's own, so neither crosses a
        // side edge and horizontal bounds expansion has nothing to cover.
        const node = graph('light').$id('both')
        expect(node.style('background-offset-x')).toBe(`${-CHIP_INSET}px ${-CHIP_INSET}px`)
        expect(node.style('background-position-x')).toBe('100% 100%')
        expect(CHIP_INSET).toBeGreaterThan(0)
    })

})

describe('edge routing', () => {
    it('routes edges as rounded taxi lines rather than straight center-to-center lines', () => {
        const cy = graph('light')
        expect(cy.$id('e').style('curve-style')).toBe('round-taxi')
        expect(cy.$id('e').style('taxi-direction')).toBe('vertical')
    })

    it('opens the corners into long sweeps', () => {
        // A radius this far above the node height is what turns the orthogonal route into a
        // spline-like curve; cytoscape clamps it to whatever each segment can take.
        expect(Number(graph('light').$id('e').style('taxi-radius'))).toBe(100)
    })
})

describe('draw order', () => {
    it('draws every edge below every node, so chips are never crossed by an edge', () => {
        // Chips are node background images, so they are drawn with their node. Cytoscape puts edges
        // under nodes only within one nesting depth: an expanded region sits shallower than the edges
        // between its children, so without an explicit depth its chips end up under those edges.
        const cy = graph('light')
        const order = cy.elements().sortByZIndex().map((ele) => ele.isEdge())
        const lastEdge = order.lastIndexOf(true)
        const firstNode = order.indexOf(false)
        expect(cy.edges()).toHaveLength(2)
        expect(lastEdge).toBeLessThan(firstNode)
    })

    it('draws a raised edge over the plain ones, and still under every node', () => {
        // A gray edge crossing a colored one would otherwise cut it in two, and the trace is what the
        // eye is following. `e` is the older of the two edges, so it only comes last by its own doing.
        const cy = graph('light')
        cy.$id('e').addClass(RAISED_EDGE_CLASS)
        expect(cy.edges().sortByZIndex().map((edge) => edge.id())).toEqual(['inner', 'e'])
        const order = cy.elements().sortByZIndex().map((ele) => ele.isEdge())
        expect(order.lastIndexOf(true)).toBeLessThan(order.indexOf(false))
    })

    it('does not change the draw order with the trace colors alone', () => {
        // A change of `z-index` makes cytoscape sort all elements before the next draw.
        // `CytographRendering` adds the color classes first and `RAISED_EDGE_CLASS` one frame later,
        // so the colors paint one frame sooner.
        const cy = graph('light')
        for (const highlight of ['highlight-forward', 'highlight-backward']) {
            cy.$id('e').addClass(highlight)
            expect(cy.edges().sortByZIndex().map((edge) => edge.id()), highlight)
                .toEqual(['e', 'inner'])
            cy.$id('e').removeClass(highlight)
        }
    })
})

describe('chip geometry', () => {
    it('draws chips at the node font size', () => {
        // The node font size and the chip font size live in different modules; they have to agree
        // for a chip to read as part of the node.
        expect(graph('light').$id('plain').style('font-size')).toBe(`${CHIP_FONT_SIZE}px`)
    })
})

describe('node geometry', () => {
    it('pads operators horizontally without making them taller', () => {
        // Cytoscape pads all four sides at once, so the node's own height takes off the padding it
        // adds above and below - otherwise 8px of side padding would also grow a 20px node to 36px.
        const node = graph('light').$id('plain')
        expect(NODE_PADDING_X).toBeGreaterThan(0)
        expect(node.style('padding')).toBe(`${NODE_PADDING_X}px`)
        // Get that arithmetic wrong and side padding silently makes every operator taller.
        expect(node.outerHeight()).toBe(NODE_OUTER_HEIGHT)
    })

    it('gives a collapsed circuit region a first row for its counter chip', () => {
        // The chip sits inside the node, in a row of its own above the text, so the node is taller
        // than an operator by that row.
        const cy = graph('light')
        expect(cy.$id('collapsed').outerHeight()).toBe(COLLAPSED_REGION_OUTER_HEIGHT)
        expect(COLLAPSED_REGION_OUTER_HEIGHT).toBeGreaterThan(NODE_OUTER_HEIGHT)
        // Tall enough for the chip and the gap around it. The stylesheet cannot read chip geometry,
        // the two modules importing each other, so this is where the two are held together.
        expect(COLLAPSED_REGION_OUTER_HEIGHT - NODE_OUTER_HEIGHT)
            .toBeGreaterThanOrEqual(BADGE_HEIGHT + 2 * CHIP_INSET)
        // An expanded region ignores `height` and sizes itself to its children instead.
        expect(cy.$id('region').outerHeight()).not.toBe(COLLAPSED_REGION_OUTER_HEIGHT)
    })

    it('rounds a circuit region more softly than the operators it holds', () => {
        const cy = graph('light')
        expect(cy.$id('plain').style('shape')).toBe('round-rectangle')
        expect(cy.$id('plain').style('corner-radius')).toBe(`${NODE_CORNER_RADIUS}px`)
        expect(cy.$id('collapsed').style('corner-radius')).toBe(`${COLLAPSED_REGION_CORNER_RADIUS}px`)
        expect(cy.$id('region').style('corner-radius')).toBe(`${REGION_CORNER_RADIUS}px`)
        // Every node is a round rectangle, so the radius is one of the two things that separate an
        // operator from a circuit region, the border style below being the other.
        expect(NODE_CORNER_RADIUS).toBeLessThan(COLLAPSED_REGION_CORNER_RADIUS)
    })

    it('dashes the region border and leaves operator borders solid', () => {
        const cy = graph('light')
        expect(cy.$id('region').style('border-style')).toBe('dashed')
        expect(cy.$id('plain').style('border-style')).toBe('solid')
        expect(cy.$id('collapsed').style('border-style')).toBe('solid')
    })
})

describe('expanded regions', () => {
    it('fills a region from the palette without waiting for a graph update', () => {
        // Keyed on the `depth` attribute instead, which is only written from the second graph update
        // onwards, a freshly rendered region would keep cytoscape's default grey.
        for (const theme of ['light', 'dark'] as const) {
            const region = graph(theme).$id('region')
            expect(region.isParent()).toBe(true)
            expect(region.style('background-color')).toBe(hexToRgb(DIAGRAM_PALETTES[theme].region))
            expect(Number(region.style('background-opacity'))).toBeCloseTo(REGION_OPACITY, 5)
        }
    })

    it('leaves the operators inside a region opaque', () => {
        const cy = graph('light')
        expect(Number(cy.$id('inside').style('background-opacity'))).toBe(1)
    })
})

describe('the width of an expanded region', () => {
    /** A region named `label` that holds one operator `child` pixels wide, sized the way the diagram
     *  sizes it: `min_width` comes from the node definition, everything else from the stylesheet. */
    const region = (label: string, leafCount: number, child: number) =>
        cytoscape({
            headless: true,
            styleEnabled: true,
            style: buildGraphStyle('light'),
            elements: {
                nodes: [
                    {
                        data: {
                            id: 'region',
                            label,
                            operator: label,
                            has_children: true,
                            text_width: labelWidth(label),
                            min_width: regionMinWidth(label, leafCount),
                            chips: nodeChips(false, leafCount, 'light')
                        }
                    },
                    {
                        data: {
                            id: 'inside',
                            label: 'inside',
                            text_width: child,
                            parent: 'region',
                            chips: nodeChips(false, 0, 'light')
                        },
                        position: { x: 0, y: 0 }
                    }
                ]
            }
        }).$id('region')

    const LONG = 'region shard_by_index_and_key'
    const COUNT = 12

    it('pads a narrow region out to what its own name needs', () => {
        // Cytoscape sizes a parent from its children and ignores the parent's own label, so a region
        // around one short operator would come out narrower than the name in its top band, and the name
        // would overflow both borders and run under the counter chip.
        const node = region(LONG, COUNT, 30)
        expect(node.children().boundingBox().w).toBeLessThan(labelWidth(LONG))
        expect(node.outerWidth())
            .toBeGreaterThanOrEqual(labelWidth(LONG) + 2 * badgePillWidth(formatLeafCount(COUNT)))
        // The floor is measured against the children, so the region adds its padding around it. That
        // padding lies inside the band holding the name, which is why `regionMinWidth` subtracts it.
        expect(node.numericStyle('padding')).toBe(REGION_PADDING)
    })

    it('leaves a region sized by the nodes inside it when those are the wider ones', () => {
        // The name of this region fits within the operator it holds, so the floor never applies and the
        // region stays exactly as wide as that operator.
        const node = region('region r', 1, 600)
        // `width()` is the node's body, which for a parent is the box around its children. The padding
        // drawn outside that box is covered by `node geometry` above.
        expect(node.width()).toBeCloseTo(node.children().boundingBox().w, 0)
    })

    it('adds every extra pixel on the right, the side ELK reserves it on', () => {
        // By default cytoscape splits the extra width evenly between the two sides, which would push
        // the region into whatever the layout placed to its left.
        const node = region(LONG, COUNT, 30)
        const inside = node.children().boundingBox()
        // Both distances are measured from the body rather than the padded box, so that they are
        // comparable: the left edge stays on the nodes inside, and the whole widening is on the right.
        expect(inside.x1 - (node.position().x - node.width() / 2)).toBeCloseTo(0, 0)
        expect(node.position().x + node.width() / 2 - inside.x2)
            .toBeCloseTo(regionMinWidth(LONG, COUNT) - inside.w, 0)
    })
})

describe('the top of an expanded region', () => {
    /** Region `outer` holds an operator and region `inner`, which holds one more operator. Both
     *  operators have a code chip. */
    const nested = () => {
        const cy = cytoscape({
            headless: true,
            styleEnabled: true,
            style: buildGraphStyle('light'),
            elements: {
                nodes: [
                    { data: { id: 'outer', label: 'outer', has_children: true, min_width: 0, min_height: 0, chips: nodeChips(false, 9, 'light') } },
                    { data: { id: 'inner', label: 'inner', parent: 'outer', has_children: true, min_width: 0, min_height: 0, chips: nodeChips(false, 4, 'light') } },
                    { data: { id: 'a', label: 'a', text_width: 200, parent: 'outer', has_source: true, chips: nodeChips(true, 0, 'light') } },
                    { data: { id: 'b', label: 'b', text_width: 200, parent: 'inner', has_source: true, chips: nodeChips(true, 0, 'light') } }
                ]
            }
        })
        // `a` is the top node of `outer`, and `inner` is below it.
        cy.$id('a').position({ x: 0, y: 0 })
        cy.$id('b').position({ x: 0, y: 100 })
        return cy
    }
    const box = (node: cytoscape.NodeSingular) => ({
        top: node.position().y - node.outerHeight() / 2,
        bottom: node.position().y + node.outerHeight() / 2,
        left: node.position().x - node.outerWidth() / 2,
        right: node.position().x + node.outerWidth() / 2
    })
    /** The bottom of the chip row of `region`, the row along its top edge with its counter chip and name. */
    const chipRowBottom = (region: cytoscape.NodeSingular) => box(region).top + CHIP_INSET + BADGE_HEIGHT
    /** The top of the code chip of `node`. */
    const codeChipTop = (node: cytoscape.NodeSingular) => box(node).top - CODE_CHIP_HEIGHT

    it('keeps the code chip of the top node below the chip row of the region', () => {
        const cy = nested()
        // Without `REGION_CHIP_ROW_ROOM`, the code chip of `a` reaches into the chip row of `outer`.
        expect(codeChipTop(cy.$id('a'))).toBeLessThan(chipRowBottom(cy.$id('outer')))
        reserveRegionChipRows(cy)
        expect(codeChipTop(cy.$id('a'))).toBeGreaterThanOrEqual(chipRowBottom(cy.$id('outer')) + CHIP_INSET)
        expect(codeChipTop(cy.$id('b'))).toBeGreaterThanOrEqual(chipRowBottom(cy.$id('inner')) + CHIP_INSET)
    })

    it('adds the room at the top of each region, and nowhere else', () => {
        const cy = nested()
        const before = { outer: box(cy.$id('outer')), inner: box(cy.$id('inner')) }
        reserveRegionChipRows(cy)
        const inner = box(cy.$id('inner'))
        expect(inner.top).toBeCloseTo(before.inner.top - REGION_CHIP_ROW_ROOM, 6)
        expect(inner.bottom).toBeCloseTo(before.inner.bottom, 6)
        expect(inner.left).toBeCloseTo(before.inner.left, 6)
        expect(inner.right).toBeCloseTo(before.inner.right, 6)
        const outer = box(cy.$id('outer'))
        expect(outer.top).toBeCloseTo(before.outer.top - REGION_CHIP_ROW_ROOM, 6)
        expect(outer.bottom).toBeCloseTo(before.outer.bottom, 6)
        expect(outer.left).toBeCloseTo(before.outer.left, 6)
        expect(outer.right).toBeCloseTo(before.outer.right, 6)
    })

    it('follows the nodes when a later layout moves them', () => {
        const cy = nested()
        reserveRegionChipRows(cy)
        cy.$id('b').position({ x: 0, y: 300 })
        reserveRegionChipRows(cy)
        const outer = cy.$id('outer')
        expect(outer.height()).toBeCloseTo(outer.children().boundingBox({ includeOverlays: false }).h + REGION_CHIP_ROW_ROOM, 6)
    })
})

describe('palettes', () => {
    it('takes node borders and edges from their own palette entries', () => {
        // Node text is not among them: `nodeText.ts` draws it and picks its colors.
        for (const theme of ['light', 'dark'] as const) {
            const cy = graph(theme)
            const palette = DIAGRAM_PALETTES[theme]
            expect(cy.$id('plain').style('border-color')).toBe(hexToRgb(palette.border))
            expect(cy.$id('e').style('line-color')).toBe(hexToRgb(palette.edge))
            expect(cy.$id('e').style('target-arrow-color')).toBe(hexToRgb(palette.edge))
        }
    })

    it('keeps borders and edges on separate entries, whatever the two hold', () => {
        // They hold the same color today, so nothing above can tell which entry either side reads.
        // Repainting one entry is what shows that: only its own side of the diagram follows.
        const palette = DIAGRAM_PALETTES.light
        const [border, edge] = [palette.border, palette.edge]
        try {
            palette.edge = '#123456'
            const cy = graph('light')
            expect(cy.$id('e').style('line-color')).toBe(hexToRgb('#123456'))
            expect(cy.$id('plain').style('border-color')).toBe(hexToRgb(border))

            palette.border = '#654321'
            const repainted = graph('light')
            expect(repainted.$id('plain').style('border-color')).toBe(hexToRgb('#654321'))
            expect(repainted.$id('e').style('line-color')).toBe(hexToRgb('#123456'))
        } finally {
            palette.border = border
            palette.edge = edge
        }
    })

    it('measures the node label at the id weight but never draws it', () => {
        // The text is painted as two runs by `nodeText.ts`; the label stays only to size the node.
        // Measured at the heavier run's weight, so the drawn text cannot overflow it.
        const node = graph('light').$id('plain')
        expect(node.style('label')).toBe('plain map')
        expect(Number(node.style('text-opacity'))).toBe(0)
        expect(String(node.style('font-weight'))).toBe(`${ID_FONT_WEIGHT}`)
    })

    it('leaves no light-theme color hardcoded in the dark stylesheet', () => {
        // Every color in the sheet has to come from the palette, otherwise a black border or a
        // white node fill survives the switch to dark.
        const sheet = JSON.stringify(buildGraphStyle('dark'))
        for (const hardcoded of ['black', 'white', DIAGRAM_PALETTES.light.region]) {
            expect(sheet).not.toContain(hardcoded)
        }
    })
})

describe('labelWidth', () => {
    afterEach(() => {
        vi.unstubAllGlobals()
        vi.resetModules()
    })

    /** A fresh `diagramTheme` whose canvas records every text it is asked to measure. The module is
     *  re-imported so that it builds its context against the stub rather than reusing the one the
     *  tests above already resolved. */
    const measuringModule = async (measured: Array<string>) => {
        vi.resetModules()
        vi.stubGlobal('document', {
            createElement: () => ({
                getContext: () => ({
                    font: '',
                    measureText: (text: string) => {
                        measured.push(text)
                        return { width: text.length * 7.3 }
                    }
                })
            })
        })
        return await import('./diagramTheme.js')
    }

    it('measures a label once and answers the repeats from the memo', async () => {
        // A label is measured on every graph build, and a diagram of thousands of nodes repeats the
        // same operator names throughout.
        const measured: Array<string> = []
        const theme = await measuringModule(measured)
        expect(theme.labelWidth('map 12')).toBe(Math.ceil(6 * 7.3))
        expect(theme.labelWidth('map 12')).toBe(Math.ceil(6 * 7.3))
        expect(theme.labelWidth('filter 3')).toBe(Math.ceil(8 * 7.3))
        expect(measured).toEqual(['map 12', 'filter 3'])
    })

    it('falls back to a glyph estimate where there is no document to measure in', () => {
        // The tests above run in that state, and nothing is drawn there either.
        expect(labelWidth('')).toBe(0)
        // A width per character, rounded up: twice the text is within a pixel of twice the width.
        expect(labelWidth('abcdabcd') - 2 * labelWidth('abcd')).toBeLessThanOrEqual(1)
        expect(labelWidth('abcd')).toBeGreaterThan(labelWidth('ab'))
    })
})

const hexToRgb = (hex: string): string => {
    const n = Number.parseInt(hex.slice(1), 16)
    return `rgb(${(n >> 16) & 255},${(n >> 8) & 255},${n & 255})`
}
