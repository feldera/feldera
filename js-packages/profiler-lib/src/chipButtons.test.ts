// The chip boxes are exercised through a headless cytoscape instance carrying the real stylesheet:
// they are computed from the resolved style, so a placement change in `chips.ts` has to move them
// too. A headless instance measures no text, which leaves every node as wide as its padding - that
// affects where a box sits, never how it is built, and every assertion below is written against the
// edges the chips are anchored to.

import cytoscape, { type Core, type NodeSingular } from 'cytoscape'
import { describe, expect, it, vi } from 'vitest'
import {
    badgePillWidth,
    BADGE_CANVAS_WIDTH,
    CHIP_HEIGHT,
    CHIP_INSET,
    CHIP_NONE,
    CODE_CHIP_WIDTH,
    nodeChips
} from './chips.js'
import { chipAt, chipBox, chipUnder, installChipButtons, refreshChips } from './chipButtons.js'
import { buildGraphStyle, type DiagramTheme } from './diagramTheme.js'

const COUNT = 7

/** Nodes are spread out after construction: a headless instance runs no layout, so every position
 *  given in an element definition stays where cytoscape put it - all of them on the origin, one on
 *  top of another. */
const POSITIONS: Record<string, { x: number, y: number }> = {
    code: { x: 0, y: 0 },
    bare: { x: 200, y: 0 },
    collapsed: { x: 400, y: 0 },
    inside: { x: 600, y: 0 }
}

const graph = (theme: DiagramTheme = 'light') => {
    const cy = cytoscape({
        headless: true,
        styleEnabled: true,
        style: buildGraphStyle(theme),
        elements: {
            nodes: [
                // An operator with source and nothing to count: code chip only.
                {
                    data: { id: 'code', label: 'code', has_source: true, leaf_count: 0, chips: nodeChips(true, 0, theme) }
                },
                // An operator with neither.
                {
                    data: { id: 'bare', label: 'bare', leaf_count: 0, chips: nodeChips(false, 0, theme) }
                },
                // A collapsed composite carrying both.
                {
                    data: {
                        id: 'collapsed',
                        label: 'collapsed',
                        has_source: true,
                        has_children: true,
                        leaf_count: COUNT,
                        chips: nodeChips(true, COUNT, theme)
                    }
                },
                // An expanded region and the operator inside it, both carrying chips.
                {
                    data: {
                        id: 'region',
                        label: 'region',
                        has_children: true,
                        leaf_count: COUNT,
                        chips: nodeChips(false, COUNT, theme)
                    }
                },
                {
                    data: {
                        id: 'inside',
                        label: 'inside',
                        parent: 'region',
                        has_source: true,
                        leaf_count: 0,
                        chips: nodeChips(true, 0, theme)
                    }
                }
            ],
            edges: []
        }
    })
    for (const [id, position] of Object.entries(POSITIONS)) {
        cy.$id(id).position(position)
    }
    return cy
}

/** Which node's chip is under a point, as plain strings: a failing `expect` prints what it received,
 *  and a cytoscape element carries the whole graph with it. */
const under = (cy: Core, x: number, y: number): string => {
    const hit = chipUnder(cy, x, y)
    return hit === null ? 'nothing' : `${hit.node.id()}:${hit.slot}`
}

/** The node's own box, the one cytoscape draws and places background images against. */
const body = (node: NodeSingular) => {
    const padding = Number(node.numericStyle('padding'))
    const position = node.position()
    return {
        right: position.x + node.width() / 2 + padding,
        top: position.y - node.height() / 2 - padding
    }
}

const center = (box: { x1: number, y1: number, x2: number, y2: number }) => ({
    x: (box.x1 + box.x2) / 2,
    y: (box.y1 + box.y2) / 2
})

describe('chipBox', () => {
    it('rests the code chip on the top edge, its right edge just inside the node', () => {
        const node = graph().$id('code')
        const box = chipBox(node, 'code')!
        const { right, top } = body(node)
        expect(box.y2).toBeCloseTo(top, 5)
        expect(box.y2 - box.y1).toBeCloseTo(CHIP_HEIGHT, 5)
        expect(box.x2).toBeCloseTo(right - CHIP_INSET, 5)
        expect(box.x2 - box.x1).toBeCloseTo(CODE_CHIP_WIDTH, 5)
    })

    it('puts the counter inside the top edge, in the same right-hand column', () => {
        const node = graph().$id('collapsed')
        const counter = chipBox(node, 'counter')!
        const code = chipBox(node, 'code')!
        const { right, top } = body(node)
        expect(counter.y1).toBeCloseTo(top + CHIP_INSET, 5)
        expect(counter.y2 - counter.y1).toBeCloseTo(CHIP_HEIGHT, 5)
        expect(counter.x2).toBeCloseTo(right - CHIP_INSET, 5)
        // Stacked, with the gap the inset is: the two never touch.
        expect(counter.y1 - code.y2).toBeCloseTo(CHIP_INSET, 5)
        expect(counter.x2).toBeCloseTo(code.x2, 5)
    })

    it('measures the counter by its pill, not by the canvas it is drawn on', () => {
        // The canvas is sized for the widest count there can be and the rest of it is transparent;
        // treating the canvas as the button would put the cursor on empty space left of the pill.
        const box = chipBox(graph().$id('collapsed'), 'counter')!
        expect(box.x2 - box.x1).toBeCloseTo(badgePillWidth(String(COUNT)), 5)
        expect(box.x2 - box.x1).toBeLessThan(BADGE_CANVAS_WIDTH)
    })

    it('reports no box for a slot that carries no image', () => {
        const cy = graph()
        expect(chipBox(cy.$id('code'), 'counter')).toBeNull()
        expect(chipBox(cy.$id('bare'), 'code')).toBeNull()
        expect(chipBox(cy.$id('bare'), 'counter')).toBeNull()
        // Which is what the stylesheet leaves in an empty slot.
        expect(nodeChips(false, 0, 'light')).toEqual([CHIP_NONE, CHIP_NONE])
    })

    it('follows the box of the region whose padding band the counter sits in', () => {
        const cy = graph()
        const region = cy.$id('region')
        const box = chipBox(region, 'counter')!
        const { right, top } = body(region)
        expect(box.x2).toBeCloseTo(right - CHIP_INSET, 5)
        expect(box.y1).toBeCloseTo(top + CHIP_INSET, 5)
        // A region is sized by its children, so this is a different box from any node's above.
        expect(box.x2).not.toBeCloseTo(chipBox(cy.$id('inside'), 'code')!.x2, 5)
    })
})

describe('chipAt', () => {
    it('hits a chip over its pill and nowhere else', () => {
        const node = graph().$id('collapsed')
        const counter = chipBox(node, 'counter')!
        expect(chipAt(node, center(counter).x, center(counter).y)).toBe('counter')
        const code = chipBox(node, 'code')!
        expect(chipAt(node, center(code).x, center(code).y)).toBe('code')
        // In the gap between the two, and in the node's own text row below them.
        expect(chipAt(node, center(counter).x, code.y2 + CHIP_INSET / 2)).toBeNull()
        expect(chipAt(node, center(counter).x, counter.y2 + 1)).toBeNull()
    })

    it('misses the transparent part of the counter canvas', () => {
        const node = graph().$id('collapsed')
        const counter = chipBox(node, 'counter')!
        // Left of the pill, still inside the canvas that carries it.
        expect(BADGE_CANVAS_WIDTH).toBeGreaterThan(counter.x2 - counter.x1)
        expect(chipAt(node, counter.x1 - 2, center(counter).y)).toBeNull()
    })
})

describe('chipUnder', () => {
    it('finds the chip whatever node it belongs to', () => {
        const cy = graph()
        for (const [id, slot] of [['code', 'code'], ['collapsed', 'counter'], ['region', 'counter']] as const) {
            const box = chipBox(cy.$id(id), slot)!
            expect(under(cy, center(box).x, center(box).y)).toBe(`${id}:${slot}`)
        }
    })

    it('prefers a child over the region around it, the order the two are drawn in', () => {
        // A region's counter shares the padding band above its children with the code chips those
        // children hang into it; the child is drawn over the region, so the child's chip is the one
        // the pointer is on.
        const cy = graph()
        const code = chipBox(cy.$id('inside'), 'code')!
        const counter = chipBox(cy.$id('region'), 'counter')!
        const x = (Math.max(code.x1, counter.x1) + Math.min(code.x2, counter.x2)) / 2
        const y = (Math.max(code.y1, counter.y1) + Math.min(code.y2, counter.y2)) / 2
        // The fixture has to actually overlap here, or the preference is untested.
        expect(chipAt(cy.$id('inside'), x, y)).toBe('code')
        expect(chipAt(cy.$id('region'), x, y)).toBe('counter')
        expect(under(cy, x, y)).toBe('inside:code')
    })

    it('finds nothing over empty space or over a node without chips', () => {
        const cy = graph()
        expect(under(cy, 10_000, 10_000)).toBe('nothing')
        const bare = cy.$id('bare')
        expect(under(cy, bare.position().x, bare.position().y)).toBe('nothing')
    })

    it('ignores a node that is not on screen', () => {
        // The circuit's root node is one of those: the stylesheet gives it `display: none`.
        for (const hide of [{ visibility: 'hidden' }, { display: 'none' }]) {
            const cy = graph()
            const box = chipBox(cy.$id('collapsed'), 'counter')!
            expect(under(cy, center(box).x, center(box).y)).toBe('collapsed:counter')
            cy.$id('collapsed').style(hide)
            expect(under(cy, center(box).x, center(box).y), JSON.stringify(hide)).toBe('nothing')
        }
    })
})

describe('refreshChips', () => {
    it('shows the count when the node is not hovered', () => {
        const node = graph().$id('collapsed')
        refreshChips(node, 'light')
        expect(node.data('chips')).toEqual(nodeChips(true, COUNT, 'light'))
    })

    it('offers a collapsed composite the expand control and an expanded region the collapse one', () => {
        const cy = graph()
        refreshChips(cy.$id('collapsed'), 'light', true)
        expect(cy.$id('collapsed').data('chips')[1]).toBe(nodeChips(false, COUNT, 'light', 'expand')[1])
        refreshChips(cy.$id('region'), 'light', true)
        expect(cy.$id('region').data('chips')[1]).toBe(nodeChips(false, COUNT, 'light', 'collapse')[1])
        // The two controls are not the same image, or the button would say nothing about direction.
        expect(cy.$id('collapsed').data('chips')[1]).not.toBe(cy.$id('region').data('chips')[1])
    })

    it('leaves a node with nothing to count without a control', () => {
        const node = graph().$id('code')
        refreshChips(node, 'light', true)
        expect(node.data('chips')[1]).toBe(CHIP_NONE)
    })

    it('rewrites the images for the palette, which each chip carries inside it', () => {
        const node = graph().$id('collapsed')
        refreshChips(node, 'dark')
        expect(node.data('chips')).toEqual(nodeChips(true, COUNT, 'dark'))
    })
})

/** A core that records what `installChipButtons` binds, delegating the graph itself to a real headless
 *  instance. Cytoscape's own emitter cannot deliver a synthetic pointer position, and the press is bound
 *  to the container rather than to the emitter at all. The renderer projects a client position onto the
 *  graph unchanged, so a test presses at the coordinates a chip box is given in. */
const harness = (cy: Core) => {
    const listeners: Record<string, (event: unknown) => void> = {}
    const container = {
        style: { cursor: '' },
        addEventListener: (type: string, handler: (event: unknown) => void) => {
            listeners[type] = handler
        }
    } as unknown as HTMLElement
    // biome-ignore lint/complexity/noBannedTypes: whatever cytoscape hands a handler
    const bound: Array<{ events: string, handler: Function }> = []
    const core = {
        container: () => container,
        nodes: () => cy.nodes(),
        renderer: () => ({ projectIntoViewport: (x: number, y: number) => [x, y] }),
        on: (events: string, a: unknown, b?: unknown) => {
            bound.push({ events, handler: (typeof a === 'string' ? b : a) as () => void })
        }
    } as unknown as Core
    return {
        core,
        container,
        fire: (events: string, event: unknown) => {
            for (const entry of bound.filter((e) => e.events === events)) {
                (entry.handler as (e: unknown) => void)(event)
            }
        },
        /** A mouse press or release at a point on the graph, returning what the handler was allowed to
         *  do with it: whether it was kept from cytoscape, whose own listener is bound behind this one. */
        mouse: (type: 'mousedown' | 'mouseup', point: { x: number, y: number }, button = 0) => {
            const event = {
                clientX: point.x,
                clientY: point.y,
                button,
                stopPropagation: vi.fn(),
                preventDefault: vi.fn()
            }
            listeners[type]?.(event)
            return { stopped: event.stopPropagation.mock.calls.length > 0 }
        }
    }
}

const at = (cy: Core, id: string, slot: 'code' | 'counter') => {
    const box = chipBox(cy.$id(id) as unknown as NodeSingular, slot)!
    return { position: center(box) }
}

describe('installChipButtons', () => {
    const actions = () => ({ onSource: vi.fn(), onToggle: vi.fn() })

    it('points the cursor at a chip and leaves it alone everywhere else', () => {
        const cy = graph()
        const { core, container, fire } = harness(cy)
        installChipButtons(core, () => 'light', actions())

        fire('mousemove', at(cy, 'code', 'code'))
        expect(container.style.cursor).toBe('pointer')
        fire('mousemove', { position: { x: 10_000, y: 10_000 } })
        expect(container.style.cursor).toBe('')
    })

    it('presses the chip on release, and keeps the press from cytoscape', () => {
        // Cytoscape hit-tests a press by the node's own shape, and every chip is drawn outside one: a
        // press that reached cytoscape would land on whatever the chip is drawn over - a whole region,
        // for a code chip resting in its top band - which it would then mark, select and drag.
        const cy = graph()
        const { core, mouse } = harness(cy)
        const handlers = actions()
        installChipButtons(core, () => 'light', handlers)
        const chip = at(cy, 'collapsed', 'code').position

        expect(mouse('mousedown', chip).stopped).toBe(true)
        // Nothing is pressed until it is let go of, as it is for every other button.
        expect(handlers.onSource).not.toHaveBeenCalled()
        mouse('mouseup', chip)
        expect(handlers.onSource).toHaveBeenCalledWith('collapsed')
        expect(handlers.onToggle).not.toHaveBeenCalled()
    })

    it('leaves a press that is not on a chip to cytoscape', () => {
        const cy = graph()
        const { core, mouse } = harness(cy)
        const handlers = actions()
        installChipButtons(core, () => 'light', handlers)
        const elsewhere = { x: 10_000, y: 10_000 }

        expect(mouse('mousedown', elsewhere).stopped).toBe(false)
        mouse('mouseup', elsewhere)
        // And a press of another button, which is not what a button answers.
        expect(mouse('mousedown', at(cy, 'collapsed', 'counter').position, 2).stopped).toBe(false)
        mouse('mouseup', at(cy, 'collapsed', 'counter').position, 2)
        expect(handlers.onSource).not.toHaveBeenCalled()
        expect(handlers.onToggle).not.toHaveBeenCalled()
    })

    it('cancels a press let go of anywhere but the chip it started on', () => {
        const cy = graph()
        const { core, mouse } = harness(cy)
        const handlers = actions()
        installChipButtons(core, () => 'light', handlers)

        mouse('mousedown', at(cy, 'collapsed', 'code').position)
        mouse('mouseup', { x: 10_000, y: 10_000 })
        // Including the other chip of the same node, which is a different button.
        mouse('mousedown', at(cy, 'collapsed', 'code').position)
        mouse('mouseup', at(cy, 'collapsed', 'counter').position)
        expect(handlers.onSource).not.toHaveBeenCalled()
        expect(handlers.onToggle).not.toHaveBeenCalled()
    })

    it('dispatches the action of the chip tapped, and only over a chip', () => {
        // The tap is touch and pen: cytoscape routes both through its own touch handling and reports
        // them as one, and a press it never saw reports nothing to answer here.
        const cy = graph()
        const { core, fire } = harness(cy)
        const handlers = actions()
        installChipButtons(core, () => 'light', handlers)

        fire('tap', at(cy, 'collapsed', 'code'))
        expect(handlers.onSource).toHaveBeenCalledWith('collapsed')
        expect(handlers.onToggle).not.toHaveBeenCalled()

        fire('tap', at(cy, 'collapsed', 'counter'))
        expect(handlers.onToggle).toHaveBeenCalledWith('collapsed')
        expect(handlers.onSource).toHaveBeenCalledTimes(1)

        fire('tap', { position: { x: 10_000, y: 10_000 } })
        expect(handlers.onSource).toHaveBeenCalledTimes(1)
        expect(handlers.onToggle).toHaveBeenCalledTimes(1)
    })

    it('swaps the count for a control while the node is hovered', () => {
        const cy = graph()
        const { core, fire } = harness(cy)
        installChipButtons(core, () => 'light', actions())
        const node = cy.$id('collapsed')

        fire('mouseover', { target: node })
        expect(node.data('chips')[1]).toBe(nodeChips(false, COUNT, 'light', 'expand')[1])
        fire('mouseout', { target: node })
        expect(node.data('chips')[1]).toBe(nodeChips(false, COUNT, 'light')[1])
    })

    it('shows the count again after a layout, which moves the node out from under the pointer', () => {
        // Pressing the control is what runs a layout, and cytoscape only resolves what is hovered on
        // the next pointer move - so nothing else would take the stale control off the node.
        const cy = graph()
        const { core, fire } = harness(cy)
        installChipButtons(core, () => 'light', actions())
        const node = cy.$id('collapsed')

        fire('mouseover', { target: node })
        expect(node.data('chips')[1]).not.toBe(nodeChips(false, COUNT, 'light')[1])
        fire('layoutstop', {})
        expect(node.data('chips')[1]).toBe(nodeChips(false, COUNT, 'light')[1])
    })

    it('survives an instance with no container to set a cursor on', () => {
        const cy = graph()
        const bound: Array<(e: unknown) => void> = []
        const core = {
            container: () => null,
            nodes: () => cy.nodes(),
            on: (_events: string, a: unknown, b?: unknown) => {
                bound.push((typeof a === 'string' ? b : a) as (e: unknown) => void)
            }
        } as unknown as Core
        installChipButtons(core, () => 'light', actions())
        expect(() => {
            for (const handler of bound) {
                handler({ position: { x: 0, y: 0 }, target: cy.$id('collapsed') })
            }
        }).not.toThrow()
    })
})
