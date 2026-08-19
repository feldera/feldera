// The stylesheet is exercised through a headless cytoscape instance, which resolves styles
// without a renderer. That is what pins the two mechanisms the diagram depends on and that fail
// silently otherwise: the per-node chip image list, and taxi edge routing.

import cytoscape from 'cytoscape'
import { describe, expect, it } from 'vitest'
import { badgePillWidth, CHIP_FONT_SIZE, CHIP_HEIGHT, CHIP_INSET, formatLeafCount, nodeChips } from './chips.js'
import {
    buildGraphStyle,
    DIAGRAM_PALETTES,
    type DiagramTheme,
    COMPOSITE_CORNER_RADIUS,
    COMPOSITE_OUTER_HEIGHT,
    ID_FONT_WEIGHT,
    labelWidth,
    NODE_CORNER_RADIUS,
    NODE_OUTER_HEIGHT,
    NODE_PADDING_X,
    REGION_CORNER_RADIUS,
    REGION_OPACITY,
    REGION_PADDING
} from './diagramTheme.js'
import { regionMinWidth } from './regionSize.js'

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
                // A composite that is still collapsed: children exist but are not on the graph.
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
        expect(cy.$id('plain').style('background-image')).toBe('none none')
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
        expect(top - box.y1).toBeGreaterThanOrEqual(CHIP_HEIGHT)
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
            expect(code, id).toBe(-CHIP_HEIGHT)
            // The counter starts just below that same edge, inside the node.
            expect(counter, id).toBe(CHIP_INSET)
            // Which leaves exactly the gap between the two that the inset is.
            expect(counter! - (code! + CHIP_HEIGHT), id).toBe(CHIP_INSET)
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

    it('gives a collapsed composite a first row for its counter chip', () => {
        // The chip sits inside the node, in a row of its own above the text, so the node is taller
        // than an operator by that row.
        const cy = graph('light')
        expect(cy.$id('collapsed').outerHeight()).toBe(COMPOSITE_OUTER_HEIGHT)
        expect(COMPOSITE_OUTER_HEIGHT).toBeGreaterThan(NODE_OUTER_HEIGHT)
        // Tall enough for the chip and the gap around it. The stylesheet cannot read chip geometry,
        // the two modules importing each other, so this is where the two are held together.
        expect(COMPOSITE_OUTER_HEIGHT - NODE_OUTER_HEIGHT)
            .toBeGreaterThanOrEqual(CHIP_HEIGHT + 2 * CHIP_INSET)
        // An expanded region ignores `height` and sizes itself to its children instead.
        expect(cy.$id('region').outerHeight()).not.toBe(COMPOSITE_OUTER_HEIGHT)
    })

    it('rounds a composite more softly than the operators it holds', () => {
        const cy = graph('light')
        expect(cy.$id('plain').style('shape')).toBe('round-rectangle')
        expect(cy.$id('plain').style('corner-radius')).toBe(`${NODE_CORNER_RADIUS}px`)
        expect(cy.$id('collapsed').style('corner-radius')).toBe(`${COMPOSITE_CORNER_RADIUS}px`)
        expect(cy.$id('region').style('corner-radius')).toBe(`${REGION_CORNER_RADIUS}px`)
        // Every node is a round rectangle, so the radius is one of the two things that separate an
        // operator from a composite, the border style below being the other.
        expect(NODE_CORNER_RADIUS).toBeLessThan(COMPOSITE_CORNER_RADIUS)
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
    /** A region named `label` holding one operator `child` px wide, sized as the diagram sizes it:
     *  `min_width` from the node definition, everything else from the stylesheet. */
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
        // A cytoscape parent is sized by its children and ignores its own label, so a region holding one
        // short operator would be narrower than the name in its top band, leaving the name running past
        // both borders and under the counter chip.
        const node = region(LONG, COUNT, 30)
        expect(node.children().boundingBox().w).toBeLessThan(labelWidth(LONG))
        expect(node.outerWidth())
            .toBeGreaterThanOrEqual(labelWidth(LONG) + 2 * badgePillWidth(formatLeafCount(COUNT)))
        // The floor is measured against the children, so the region's own padding is part of the band
        // the name is drawn in - which is what `regionMinWidth` takes it off for.
        expect(node.numericStyle('padding')).toBe(REGION_PADDING)
    })

    it('leaves a region sized by the nodes inside it when those are the wider ones', () => {
        // A region around a wide operator is as wide as that operator and no wider, its name having
        // fitted anyway.
        const node = region('region r', 1, 600)
        // The node's own body, which for a parent is the box its children take; the padding around it
        // is what `node geometry` above covers.
        expect(node.width()).toBeCloseTo(node.children().boundingBox().w, 0)
    })

    it('adds every extra pixel on the right, the side ELK reserves it on', () => {
        // Cytoscape splits the extra evenly by default, which would grow the region into whatever the
        // layout placed to its left.
        const node = region(LONG, COUNT, 30)
        const inside = node.children().boundingBox()
        // Measured on the body rather than the padded box, so the two sides are comparable: the left
        // edge stays on the nodes inside, and all of the padding out to the floor is on the right.
        expect(inside.x1 - (node.position().x - node.width() / 2)).toBeCloseTo(0, 0)
        expect(node.position().x + node.width() / 2 - inside.x2)
            .toBeCloseTo(regionMinWidth(LONG, COUNT) - inside.w, 0)
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

const hexToRgb = (hex: string): string => {
    const n = Number.parseInt(hex.slice(1), 16)
    return `rgb(${(n >> 16) & 255},${(n >> 8) & 255},${n & 255})`
}
