// Unit tests for the chip boxes and the chip buttons. The tests use a headless cytoscape instance with
// the real stylesheet, because the chip boxes are calculated from the node style. So if `chips.ts`
// moves a chip, the boxes move too. A headless instance does not measure text, so each node is only as
// wide as its padding. This changes where a box is, but not how it is calculated. The assertions below
// compare each box with the node edges that the chip is attached to.

import cytoscape, { type Core, type NodeSingular } from 'cytoscape'
import { describe, expect, it, vi } from 'vitest'
import {
    badgePillWidth,
    BADGE_CANVAS_WIDTH,
    BADGE_HEIGHT,
    CHIP_INSET,
    CODE_CHIP_HEIGHT,
    CHIP_NONE,
    CODE_CHIP_WIDTH,
    nodeChips
} from './chips.js'
import { chipAt, chipBox, hitTestChips, installChipButtons, isToggleable, refreshChips } from './chipButtons.js'
import { buildGraphStyle, type DiagramTheme } from './diagramTheme.js'

const COUNT = 7

/** A headless instance does not run a layout, so all nodes start at the origin, on top of each
 *  other. `graph` moves them to these positions after it makes them. */
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
                // An operator with SQL source and no count: only the code chip.
                {
                    data: { id: 'code', label: 'code', has_source: true, leaf_count: 0, chips: nodeChips(true, 0, theme) }
                },
                // An operator with no chips.
                {
                    data: { id: 'bare', label: 'bare', leaf_count: 0, chips: nodeChips(false, 0, theme) }
                },
                // A collapsed circuit region with both chips.
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
                // An expanded region with a counter chip, and an operator inside it with a code chip.
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

/** A region `outer` that holds a region `sub`, which holds the operator `deep`. Both regions are
 *  expanded, because a nested region is always expanded. */
const nestedGraph = (theme: DiagramTheme = 'light') => {
    const region = (id: string, parent?: string) => ({
        data: {
            id,
            label: id,
            parent,
            has_children: true,
            leaf_count: 1,
            chips: nodeChips(false, 1, theme)
        }
    })
    return cytoscape({
        headless: true,
        styleEnabled: true,
        style: buildGraphStyle(theme),
        elements: {
            nodes: [
                region('outer'),
                region('sub', 'outer'),
                { data: { id: 'deep', label: 'deep', parent: 'sub', leaf_count: 0, chips: nodeChips(false, 0, theme) } }
            ],
            edges: []
        }
    })
}

/** The chip at a point as a string, for example `collapsed:counter`. A failing `expect` prints what
 *  it got, and a string is easy to read. A cytoscape element would print the whole graph. */
const under = (cy: Core, x: number, y: number): string => {
    const hit = hitTestChips(cy, x, y)
    return hit === null ? 'nothing' : `${hit.node.id()}:${hit.slot}`
}

/** The edges of the node body. Cytoscape places background images relative to this box. */
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
        expect(box.y2 - box.y1).toBeCloseTo(CODE_CHIP_HEIGHT, 5)
        expect(box.x2).toBeCloseTo(right - CHIP_INSET, 5)
        expect(box.x2 - box.x1).toBeCloseTo(CODE_CHIP_WIDTH, 5)
    })

    it('puts the counter inside the top edge, in the same right-hand column', () => {
        const node = graph().$id('collapsed')
        const counter = chipBox(node, 'counter')!
        const code = chipBox(node, 'code')!
        const { right, top } = body(node)
        expect(counter.y1).toBeCloseTo(top + CHIP_INSET, 5)
        expect(counter.y2 - counter.y1).toBeCloseTo(BADGE_HEIGHT, 5)
        expect(counter.x2).toBeCloseTo(right - CHIP_INSET, 5)
        // The code chip is above the counter, with a gap of `CHIP_INSET`, so the two chips do not
        // touch.
        expect(counter.y1 - code.y2).toBeCloseTo(CHIP_INSET, 5)
        expect(counter.x2).toBeCloseTo(code.x2, 5)
    })

    it('measures the counter by its pill, not by its whole image', () => {
        // The counter image is wide enough for the longest count, and the part outside the pill is
        // transparent. If the whole image were the button, the pointer cursor would show on the empty
        // space left of the pill.
        const box = chipBox(graph().$id('collapsed'), 'counter')!
        expect(box.x2 - box.x1).toBeCloseTo(badgePillWidth(String(COUNT)), 5)
        expect(box.x2 - box.x1).toBeLessThan(BADGE_CANVAS_WIDTH)
    })

    it('returns no box for a chip that the node does not show', () => {
        const cy = graph()
        expect(chipBox(cy.$id('code'), 'counter')).toBeNull()
        expect(chipBox(cy.$id('bare'), 'code')).toBeNull()
        expect(chipBox(cy.$id('bare'), 'counter')).toBeNull()
        // `CHIP_NONE` is the value that the stylesheet uses when a node does not show a chip.
        expect(nodeChips(false, 0, 'light')).toEqual([CHIP_NONE, CHIP_NONE])
    })

    it('places the counter of an expanded region against the region box', () => {
        const cy = graph()
        const region = cy.$id('region')
        const box = chipBox(region, 'counter')!
        const { right, top } = body(region)
        expect(box.x2).toBeCloseTo(right - CHIP_INSET, 5)
        expect(box.y1).toBeCloseTo(top + CHIP_INSET, 5)
        // The size of a region comes from its children, so its box is different from the box of the
        // node inside it.
        expect(box.x2).not.toBeCloseTo(chipBox(cy.$id('inside'), 'code')!.x2, 5)
    })
})

describe('chipAt', () => {
    it('finds a chip only over its pill', () => {
        const node = graph().$id('collapsed')
        const counter = chipBox(node, 'counter')!
        expect(chipAt(node, center(counter).x, center(counter).y)).toBe('counter')
        const code = chipBox(node, 'code')!
        expect(chipAt(node, center(code).x, center(code).y)).toBe('code')
        // Not in the gap between the two chips, and not in the text row below them.
        expect(chipAt(node, center(counter).x, code.y2 + CHIP_INSET / 2)).toBeNull()
        expect(chipAt(node, center(counter).x, counter.y2 + 1)).toBeNull()
    })

    it('ignores the transparent part of the counter image', () => {
        const node = graph().$id('collapsed')
        const counter = chipBox(node, 'counter')!
        // Left of the pill, but still inside the counter image.
        expect(BADGE_CANVAS_WIDTH).toBeGreaterThan(counter.x2 - counter.x1)
        expect(chipAt(node, counter.x1 - 2, center(counter).y)).toBeNull()
    })
})

describe('hitTestChips', () => {
    it('finds the chip on any node', () => {
        const cy = graph()
        for (const [id, slot] of [['code', 'code'], ['collapsed', 'counter'], ['region', 'counter']] as const) {
            const box = chipBox(cy.$id(id), slot)!
            expect(under(cy, center(box).x, center(box).y)).toBe(`${id}:${slot}`)
        }
    })

    it('prefers the chip of a node over the chip of the region around it', () => {
        // Near the top edge of a region, the counter of the region can overlap the code chip of a node
        // inside it. The node is drawn on top of the region, so the pointer is on the chip of the node.
        const cy = graph()
        const code = chipBox(cy.$id('inside'), 'code')!
        const counter = chipBox(cy.$id('region'), 'counter')!
        const x = (Math.max(code.x1, counter.x1) + Math.min(code.x2, counter.x2)) / 2
        const y = (Math.max(code.y1, counter.y1) + Math.min(code.y2, counter.y2)) / 2
        // Check that the two chips really overlap at this point. If they do not, this test checks
        // nothing.
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
        // For example the root node of the circuit, which the stylesheet hides with `display: none`.
        for (const hide of [{ visibility: 'hidden' }, { display: 'none' }]) {
            const cy = graph()
            const box = chipBox(cy.$id('collapsed'), 'counter')!
            expect(under(cy, center(box).x, center(box).y)).toBe('collapsed:counter')
            cy.$id('collapsed').style(hide)
            expect(under(cy, center(box).x, center(box).y), JSON.stringify(hide)).toBe('nothing')
        }
    })
})

describe('isToggleable', () => {
    it('is true only for a top-level circuit region', () => {
        const cy = graph()
        expect(isToggleable(cy.$id('collapsed'))).toBe(true)
        expect(isToggleable(cy.$id('region'))).toBe(true)
        expect(isToggleable(cy.$id('inside'))).toBe(false)
        expect(isToggleable(cy.$id('code'))).toBe(false)
        const nested = nestedGraph()
        expect(isToggleable(nested.$id('outer'))).toBe(true)
        expect(isToggleable(nested.$id('sub'))).toBe(false)
    })
})

describe('refreshChips', () => {
    it('shows the count when the node is not hovered', () => {
        const node = graph().$id('collapsed')
        refreshChips(node, 'light')
        expect(node.data('chips')).toEqual(nodeChips(true, COUNT, 'light'))
    })

    it('shows the expand icon on a collapsed region, the collapse icon on an expanded one', () => {
        const cy = graph()
        refreshChips(cy.$id('collapsed'), 'light', true)
        expect(cy.$id('collapsed').data('chips')[1]).toBe(nodeChips(false, COUNT, 'light', 'expand')[1])
        refreshChips(cy.$id('region'), 'light', true)
        expect(cy.$id('region').data('chips')[1]).toBe(nodeChips(false, COUNT, 'light', 'collapse')[1])
        // The two icons must be different images, or the user cannot tell what a click on the chip
        // does.
        expect(cy.$id('collapsed').data('chips')[1]).not.toBe(cy.$id('region').data('chips')[1])
    })

    it('keeps the count on a nested region, because only its parent collapses', () => {
        const cy = nestedGraph()
        refreshChips(cy.$id('sub'), 'light', true)
        expect(cy.$id('sub').data('chips')).toEqual(nodeChips(false, 1, 'light'))
        refreshChips(cy.$id('outer'), 'light', true)
        expect(cy.$id('outer').data('chips')[1]).toBe(nodeChips(false, 1, 'light', 'collapse')[1])
    })

    it('shows no icon on a node without a counter', () => {
        const node = graph().$id('code')
        refreshChips(node, 'light', true)
        expect(node.data('chips')[1]).toBe(CHIP_NONE)
    })

    it('makes the images again for a new theme, because each image contains its colors', () => {
        const node = graph().$id('collapsed')
        refreshChips(node, 'dark')
        expect(node.data('chips')).toEqual(nodeChips(true, COUNT, 'dark'))
    })
})

/** A fake cytoscape core that records the listeners that `installChipButtons` adds, and uses a real
 *  headless instance for the graph. The tests cannot use the cytoscape event emitter: it cannot send
 *  a fake pointer position, and the `mousedown` and `mouseup` listeners are on the container, not on
 *  the emitter. The fake renderer returns client positions unchanged, so a test can click at the
 *  coordinates of a chip box. */
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
        /** Send a `mousedown` or `mouseup` at a point on the graph. `stopped` is true if the
         *  listener stopped the event, so that the cytoscape listener behind it does not get it. */
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

    it('shows the pointer cursor only over a chip', () => {
        const cy = graph()
        const { core, container, fire } = harness(cy)
        installChipButtons(core, () => 'light', actions())

        fire('mousemove', at(cy, 'code', 'code'))
        expect(container.style.cursor).toBe('pointer')
        fire('mousemove', { position: { x: 10_000, y: 10_000 } })
        expect(container.style.cursor).toBe('')
    })

    it('clicks the chip on mouseup, and hides the mousedown from cytoscape', () => {
        // Cytoscape finds the node under the pointer by the node shape, and the chips are outside that
        // shape. If cytoscape got the `mousedown`, it would treat it as a mouse button press on the
        // node under the chip (for a code chip, this can be a whole region), and select and drag that
        // node.
        const cy = graph()
        const { core, mouse } = harness(cy)
        const handlers = actions()
        installChipButtons(core, () => 'light', handlers)
        const chip = at(cy, 'collapsed', 'code').position

        expect(mouse('mousedown', chip).stopped).toBe(true)
        // As with any button, nothing happens before `mouseup`.
        expect(handlers.onSource).not.toHaveBeenCalled()
        mouse('mouseup', chip)
        expect(handlers.onSource).toHaveBeenCalledWith('collapsed')
        expect(handlers.onToggle).not.toHaveBeenCalled()
    })

    it('lets cytoscape handle a mousedown that is not on a chip', () => {
        const cy = graph()
        const { core, mouse } = harness(cy)
        const handlers = actions()
        installChipButtons(core, () => 'light', handlers)
        const elsewhere = { x: 10_000, y: 10_000 }

        expect(mouse('mousedown', elsewhere).stopped).toBe(false)
        mouse('mouseup', elsewhere)
        // Also a click with a button other than the left button, which chips ignore.
        expect(mouse('mousedown', at(cy, 'collapsed', 'counter').position, 2).stopped).toBe(false)
        mouse('mouseup', at(cy, 'collapsed', 'counter').position, 2)
        expect(handlers.onSource).not.toHaveBeenCalled()
        expect(handlers.onToggle).not.toHaveBeenCalled()
    })

    it('cancels the click when the mouseup is not on the chip of the mousedown', () => {
        const cy = graph()
        const { core, mouse } = harness(cy)
        const handlers = actions()
        installChipButtons(core, () => 'light', handlers)

        mouse('mousedown', at(cy, 'collapsed', 'code').position)
        mouse('mouseup', { x: 10_000, y: 10_000 })
        // This includes the other chip of the same node, which is a different button.
        mouse('mousedown', at(cy, 'collapsed', 'code').position)
        mouse('mouseup', at(cy, 'collapsed', 'counter').position)
        expect(handlers.onSource).not.toHaveBeenCalled()
        expect(handlers.onToggle).not.toHaveBeenCalled()
    })

    it('runs the action of a tapped chip, and ignores a tap outside the chips', () => {
        // Cytoscape reports a tap for touch and pen input. A mouse click does not cause a tap here,
        // because the `mousedown` listener hides it from cytoscape.
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

    it('shows an icon instead of the count while the pointer is on the node', () => {
        const cy = graph()
        const { core, fire } = harness(cy)
        installChipButtons(core, () => 'light', actions())
        const node = cy.$id('collapsed')

        fire('mouseover', { target: node })
        expect(node.data('chips')[1]).toBe(nodeChips(false, COUNT, 'light', 'expand')[1])
        fire('mouseout', { target: node })
        expect(node.data('chips')[1]).toBe(nodeChips(false, COUNT, 'light')[1])
    })

    it('shows the count again after a layout moves the node away from the pointer', () => {
        // A click on the icon starts a layout, and cytoscape finds the node under the pointer again
        // only when the pointer moves. So only the `layoutstop` listener can remove the old icon.
        const cy = graph()
        const { core, fire } = harness(cy)
        installChipButtons(core, () => 'light', actions())
        const node = cy.$id('collapsed')

        fire('mouseover', { target: node })
        expect(node.data('chips')[1]).not.toBe(nodeChips(false, COUNT, 'light')[1])
        fire('layoutstop', {})
        expect(node.data('chips')[1]).toBe(nodeChips(false, COUNT, 'light')[1])
    })

    it('does not make the counter of a nested region a button, and always shows its count', () => {
        const cy = nestedGraph()
        const { core, container, fire, mouse } = harness(cy)
        const handlers = actions()
        installChipButtons(core, () => 'light', handlers)
        const counter = at(cy, 'sub', 'counter').position

        expect(under(cy, counter.x, counter.y)).not.toBe('sub:counter')
        fire('mousemove', { position: counter })
        expect(container.style.cursor).toBe('')
        expect(mouse('mousedown', counter).stopped).toBe(false)
        mouse('mouseup', counter)
        fire('tap', { position: counter })
        expect(handlers.onToggle).not.toHaveBeenCalled()

        // A pointer on the nested region shows no icon on it, and no icon on the region around it.
        fire('mouseover', { target: cy.$id('sub') })
        expect(cy.$id('sub').data('chips')[1]).toBe(nodeChips(false, 1, 'light')[1])
        expect(cy.$id('outer').data('chips')[1]).toBe(nodeChips(false, 1, 'light')[1])
        fire('mouseover', { target: cy.$id('outer') })
        expect(cy.$id('outer').data('chips')[1]).toBe(nodeChips(false, 1, 'light', 'collapse')[1])
    })

    it('works on an instance with no container', () => {
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
