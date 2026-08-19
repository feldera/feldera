import cytoscape from 'cytoscape'
import { describe, expect, it } from 'vitest'
import { CHIP_HEIGHT, CHIP_INSET } from './chips.js'
import {
    COMPOSITE_OUTER_HEIGHT,
    DIAGRAM_PALETTES,
    HEAT_TEXT_FLIP,
    ID_FONT_WEIGHT,
    NODE_OUTER_HEIGHT
} from './diagramTheme.js'
import { installNodeText, nodeTextRuns, paintTextRuns, textCenter, type TextRun } from './nodeText.js'

/** A context that records what it was asked to draw. Each character is one unit wide, and a run set
 *  in a heavier weight twice that, so run widths are readable in the assertions. */
const fakeContext = () => ({
    font: '',
    fillStyle: '' as string,
    textAlign: 'start' as CanvasTextAlign,
    textBaseline: 'alphabetic' as CanvasTextBaseline,
    globalAlpha: 1,
    log: [] as string[],
    drawn: [] as Array<{ text: string, x: number, y: number, font: string, fill: string, alpha: number }>,
    measureText(text: string) {
        return { width: text.length * (/^[1-9]00 /.test(this.font) ? 2 : 1) }
    },
    fillText(text: string, x: number, y: number) {
        this.drawn.push({
            text,
            x,
            y,
            font: this.font,
            fill: this.fillStyle,
            alpha: this.globalAlpha
        })
    },
    save() { this.log.push('save') },
    restore() { this.log.push('restore') }
})

const runs = (heat = 0, theme: 'light' | 'dark' = 'light') =>
    nodeTextRuns('nn42', 'MapIndexedZSet', heat, theme, false)

describe('nodeTextRuns', () => {
    it('leads with the id, set apart from the operator name by its weight', () => {
        const [id, operator] = runs()
        expect(id).toMatchObject({ text: 'nn42', weight: `${ID_FONT_WEIGHT}` })
        expect(operator).toMatchObject({ text: 'MapIndexedZSet', weight: 'normal' })
        // Semibold: heavier than the operator name, lighter than bold, which at this size reads as a
        // block of ink rather than as emphasis.
        expect(ID_FONT_WEIGHT).toBeGreaterThan(400)
        expect(ID_FONT_WEIGHT).toBeLessThan(700)
    })

    it('draws the operator name in the muted color, the id in the plain one', () => {
        for (const theme of ['light', 'dark'] as const) {
            const palette = DIAGRAM_PALETTES[theme]
            const [id, operator] = runs(0, theme)
            expect(id!.color, theme).toBe(palette.text)
            expect(operator!.color, theme).toBe(palette.textMuted)
            // Muted means subordinate to the id, not invisible: the two have to differ.
            expect(operator!.color, theme).not.toBe(palette.text)
        }
    })

    it('turns both runs to the on-heat color once the fill is saturated', () => {
        // Dark text on a near-pure red fill reads as sitting in the fill rather than on it. Weight is
        // then all that separates the id from the operator name.
        const palette = DIAGRAM_PALETTES.light
        const hot = runs(HEAT_TEXT_FLIP + 1)
        expect(hot.map((r) => r.color)).toEqual([palette.textOnHeat, palette.textOnHeat])
        expect(hot[0]!.weight).not.toBe(hot[1]!.weight)
        expect(runs(HEAT_TEXT_FLIP).map((r) => r.color)).toEqual([palette.text, palette.textMuted])
    })

    it('leaves the name of an expanded region alone however hot the region is', () => {
        // A region is drawn in the region tint whatever its metric says, and it carries the metric of
        // everything inside it, so one saturated descendant must not turn the names of the regions above
        // it white.
        const palette = DIAGRAM_PALETTES.light
        const region = nodeTextRuns('region', 'shard', 100, 'light', true)
        expect(region.map((r) => r.color)).toEqual([palette.text, palette.textMuted])
    })

    it('drops the operator run from a node that has no operator name', () => {
        expect(nodeTextRuns('nn42', '', 0, 'light', false)).toHaveLength(1)
    })
})

describe('textCenter', () => {
    const position = { x: 50, y: 100 }

    it('centers the text of an operator, which is one row tall', () => {
        expect(textCenter(position, NODE_OUTER_HEIGHT, false)).toEqual(position)
    })

    it('puts it in the second row of a collapsed composite, the first being the counter chip', () => {
        const center = textCenter(position, COMPOSITE_OUTER_HEIGHT, false)
        expect(center.x).toBe(position.x)
        // The text row is the bottom `NODE_OUTER_HEIGHT` of the node: its center sits half a row above
        // the bottom edge, wherever the extra height went.
        expect(center.y).toBe(position.y + COMPOSITE_OUTER_HEIGHT / 2 - NODE_OUTER_HEIGHT / 2)
    })

    it('puts the name of an expanded region in the band along its top edge', () => {
        // Not in the middle of the region: that is where the nodes it contains are drawn.
        const height = 400
        const center = textCenter(position, height, true)
        expect(center.x).toBe(position.x)
        const fromTop = center.y - (position.y - height / 2)
        expect(fromTop).toBeLessThan(NODE_OUTER_HEIGHT)
        // On the line its counter chip sits on, so the name and the count read as one row.
        expect(fromTop).toBe(CHIP_INSET + CHIP_HEIGHT / 2)
    })
})

describe('paintTextRuns', () => {
    const twoRuns: TextRun[] = [
        { text: 'ab', weight: '600', color: '#111111' },
        { text: 'cdef', weight: 'normal', color: '#999999' }
    ]

    it('draws the runs in order, each in its own font and color', () => {
        const context = fakeContext()
        paintTextRuns(context, twoRuns, 12, 'Helvetica', 0, 0)
        expect(context.drawn.map((d) => d.text)).toEqual(['ab', 'cdef'])
        expect(context.drawn[0]!.font).toBe('600 12px Helvetica')
        expect(context.drawn[1]!.font).toBe('normal 12px Helvetica')
        expect(context.drawn.map((d) => d.fill)).toEqual(['#111111', '#999999'])
        expect(context.drawn.map((d) => d.alpha)).toEqual([1, 1])
        // Whatever it changed, it changed inside a save/restore pair.
        expect(context.log).toEqual(['save', 'restore'])
    })

    it('centers the runs as one line, separated by a space', () => {
        const context = fakeContext()
        paintTextRuns(context, twoRuns, 12, 'Helvetica', 100, 50)
        // Semibold 'ab' measures 4, 'cdef' 4, and the space between them 1.
        const total = 4 + 1 + 4
        expect(context.drawn[0]!.x).toBe(100 - total / 2)
        expect(context.drawn[1]!.x).toBe(100 - total / 2 + 4 + 1)
        // Drawn from the left of each run, on the row's own center line.
        expect(context.textAlign).toBe('left')
        expect(context.textBaseline).toBe('middle')
        expect(context.drawn.map((d) => d.y)).toEqual([50, 50])
    })

    it('measures the separator in the lighter weight, as the measured label carries it', () => {
        // The label cytoscape measures to size the node is one string at the id's weight; a separator
        // measured at that weight here would push the runs wider than that measurement.
        const context = fakeContext()
        paintTextRuns(context, twoRuns, 12, 'Helvetica', 0, 0)
        const gap = context.drawn[1]!.x - (context.drawn[0]!.x + 4)
        expect(gap).toBe(1)
    })
})

describe('installNodeText', () => {
    const headlessNode = (data: Record<string, unknown>) =>
        cytoscape({ headless: true, styleEnabled: true, elements: { nodes: [{ data }] } }).$id(
            String(data['id'])
        )

    it('wraps the renderer hook, passing every argument through', () => {
        // The hook is not part of cytoscape's documented API, so this pins the shape it is called
        // with: a context and a node first, the node's precomputed geometry after.
        const calls: unknown[][] = []
        const renderer = { drawNodeOverlay: (...args: unknown[]) => calls.push(args) }
        installNodeText({ renderer: () => renderer } as never, () => 'light')

        const context = fakeContext()
        const node = headlessNode({ id: 'nn42', operator: 'map' })
        const pos = { x: 10, y: 20 }
        renderer.drawNodeOverlay(context as never, node as never, pos as never, 40 as never, 25 as never)
        expect(calls).toEqual([[context, node, pos, 40, 25]])
        expect(context.drawn.map((d) => d.text)).toEqual(['nn42', 'map'])
    })

    it('names an expanded region as well as the nodes inside it', () => {
        const cy = cytoscape({
            headless: true,
            styleEnabled: true,
            elements: {
                nodes: [
                    { data: { id: 'region', operator: 'region', has_children: true } },
                    { data: { id: 'inside', operator: 'map', parent: 'region' } }
                ]
            }
        })
        const renderer = { drawNodeOverlay: () => {} }
        installNodeText({ renderer: () => renderer } as never, () => 'light')

        const context = fakeContext()
        renderer.drawNodeOverlay(context as never, cy.$id('region') as never)
        expect(context.drawn.map((d) => d.text)).toEqual(['region', 'region'])
        renderer.drawNodeOverlay(context as never, cy.$id('inside') as never)
        expect(context.drawn.map((d) => d.text)).toEqual(['region', 'region', 'inside', 'map'])
        // The region's name goes above its children, not across them.
        expect(context.drawn[0]!.y).toBeLessThan(context.drawn[2]!.y)
    })

    it('keeps a region name off the heat color while the nodes inside it turn', () => {
        // The two are told apart by whether the node has children, so the metric on the region itself -
        // the aggregate of what it holds, and what would flip its name - is read from a real parent.
        const cy = cytoscape({
            headless: true,
            styleEnabled: true,
            elements: {
                nodes: [
                    { data: { id: 'region', operator: 'shard', has_children: true, value: 100 } },
                    { data: { id: 'inside', operator: 'map', parent: 'region', value: 100 } }
                ]
            }
        })
        const renderer = { drawNodeOverlay: () => {} }
        installNodeText({ renderer: () => renderer } as never, () => 'light')

        const context = fakeContext()
        renderer.drawNodeOverlay(context as never, cy.$id('region') as never)
        renderer.drawNodeOverlay(context as never, cy.$id('inside') as never)
        const palette = DIAGRAM_PALETTES.light
        expect(cy.$id('region').isParent()).toBe(true)
        expect(context.drawn.map((d) => d.fill)).toEqual([
            palette.text,
            palette.textMuted,
            palette.textOnHeat,
            palette.textOnHeat
        ])
    })

    it('leaves a headless instance alone', () => {
        expect(() => installNodeText({ renderer: () => ({}) } as never, () => 'light')).not.toThrow()
    })
})
