// Chip buttons: the two corner chips are controls, not decoration. The code chip reveals the SQL
// source of the node it sits on; the counter chip expands or collapses the composite it counts, and
// shows the matching window control in place of the count while that composite is hovered.
//
// Chips are cytoscape background images, so cytoscape knows nothing about them: it hit-tests a node by
// the node's own shape and ignores `bounds-expansion`, which leaves the code chip resting above the top
// edge outside every hit region. So the boxes are computed here, with the arithmetic cytoscape places a
// background image by, read off the resolved style rather than from a second copy of the stylesheet's
// numbers.
//
// The press belongs here too, for the same reason: a mouse press over a chip is stopped before cytoscape
// can interpret it and answered when it is let go of, so nothing else in the diagram has to know that a
// press may have been meant for a button drawn over something else. Touch and pen reach cytoscape, and
// the chip they press is dispatched from the tap it reports.

import type { Core, EventObject, NodeSingular } from 'cytoscape';
import {
    badgePillWidth,
    BADGE_CANVAS_WIDTH,
    CHIP_NONE,
    CODE_CHIP_WIDTH,
    formatLeafCount,
    nodeChips
} from './chips.js';
import type { DiagramTheme } from './diagramTheme.js';

/** Which chip: slot 0 of the image list, or slot 1. */
export type ChipSlot = 'code' | 'counter';
const SLOT_INDEX: Record<ChipSlot, number> = { code: 0, counter: 1 };
const SLOTS: ChipSlot[] = ['code', 'counter'];

export interface ChipHit {
    node: NodeSingular;
    slot: ChipSlot;
}

/** A chip's box in graph coordinates. */
export interface ChipBox {
    x1: number;
    y1: number;
    x2: number;
    y2: number;
}

/** What a chip does when pressed. */
export interface ChipActions {
    /** The code chip: show the SQL source of the node it sits on. */
    onSource: (nodeId: string) => void;
    /** The counter chip: expand or collapse the composite it counts. */
    onToggle: (nodeId: string) => void;
}

/** One entry of a per-slot style list, which cytoscape resolves to its entries joined by spaces. */
const slotValue = (node: NodeSingular, property: string, index: number): string =>
    String(node.style(property)).split(' ')[index] ?? '';

const pixels = (value: string): number => Number.parseFloat(value) || 0;

/** Where an image `span` long lands along one axis of a node box `body` long, relative to that box's
 *  own left or top edge. Cytoscape reads a percentage as a share of the slack around the image and a
 *  length as absolute, with the offset adding to either. */
const place = (body: number, span: number, position: string, offset: string): number => {
    const distance = (value: string) =>
        value.endsWith('%') ? ((body - span) * pixels(value)) / 100 : pixels(value);
    return distance(position) + distance(offset);
};

/** Primitive operators inside `node`, 0 for one that holds none. */
const counted = (node: NodeSingular): number => Number(node.data('leaf_count')) || 0;

/** Width of the pill actually drawn in a slot. The code chip fills its canvas, while the counter's
 *  canvas is sized for the widest count there can be and its pill only for the count it carries. */
const pillWidth = (node: NodeSingular, slot: ChipSlot): number =>
    slot === 'code' ? CODE_CHIP_WIDTH : badgePillWidth(formatLeafCount(counted(node)));

/** Box of one chip of `node`, or `null` when that slot carries no image. */
export function chipBox(node: NodeSingular, slot: ChipSlot): ChipBox | null {
    const index = SLOT_INDEX[slot]!;
    if (slotValue(node, 'background-image', index) === CHIP_NONE) {
        return null;
    }
    // The body box cytoscape draws the node and places its images against: the node's own size plus
    // its padding, the border straddling the edge of it.
    const padding = Number(node.numericStyle('padding')) || 0;
    const bodyWidth = node.width() + 2 * padding;
    const bodyHeight = node.height() + 2 * padding;
    const canvasWidth = pixels(slotValue(node, 'background-width', index));
    const canvasHeight = pixels(slotValue(node, 'background-height', index));
    const position = node.position();
    const left = position.x - bodyWidth / 2
        + place(bodyWidth, canvasWidth,
            slotValue(node, 'background-position-x', index),
            slotValue(node, 'background-offset-x', index));
    const top = position.y - bodyHeight / 2
        + place(bodyHeight, canvasHeight,
            slotValue(node, 'background-position-y', index),
            slotValue(node, 'background-offset-y', index));
    // The pill is right-aligned in its canvas, and the rest of the canvas is transparent - no part of
    // the button.
    const right = left + canvasWidth;
    return { x1: right - pillWidth(node, slot), y1: top, x2: right, y2: top + canvasHeight };
}

/** Whether a node is on screen at all. Not cytoscape's `visible()`, which also asks whether the node
 *  takes up space - a node whose width is a measured label is zero-wide until something measures it. */
const shown = (node: NodeSingular): boolean =>
    String(node.style('display')) !== 'none' && String(node.style('visibility')) === 'visible';

const holds = (box: ChipBox, x: number, y: number): boolean =>
    x >= box.x1 && x <= box.x2 && y >= box.y1 && y <= box.y2;

/** Which chip of `node`, if any, is under a point in graph coordinates. */
export function chipAt(node: NodeSingular, x: number, y: number): ChipSlot | null {
    for (const slot of SLOTS) {
        const box = chipBox(node, slot);
        if (box !== null && holds(box, x, y)) {
            return slot;
        }
    }
    return null;
}

/** The chip under a point in graph coordinates, wherever on the graph it is. The bounding box rejects
 *  all but a handful of nodes before any style is read, widened to the left because a chip wider than the
 *  node carrying it reaches past that box - `bounds-expansion` covers the top only, since every pixel of
 *  it is also dead space the ELK layout spaces nodes by. A child wins over the region around it, matching
 *  the order the two are drawn in: a region's counter shares the padding band that the code chips of its
 *  topmost children reach into. */
export function chipUnder(cy: Core, x: number, y: number): ChipHit | null {
    let region: ChipHit | null = null;
    for (const node of cy.nodes().toArray()) {
        const box = node.boundingBox();
        if (!shown(node)
            || x < box.x1 - BADGE_CANVAS_WIDTH || x > box.x2 || y < box.y1 || y > box.y2) {
            continue;
        }
        const slot = chipAt(node, x, y);
        if (slot === null) {
            continue;
        }
        if (!node.isParent()) {
            return { node, slot };
        }
        region ??= { node, slot };
    }
    return region;
}

/** Write the chip images of `node` for the palette, showing the window control in the counter slot when
 *  `hovered`: a square to expand a composite drawn collapsed, a dash to collapse one drawn expanded.
 *  Also how a theme change reaches a node's chips. */
export function refreshChips(node: NodeSingular, theme: DiagramTheme, hovered = false): void {
    const control = node.isParent() ? 'collapse' : 'expand';
    node.data(
        'chips',
        nodeChips(Boolean(node.data('has_source')), counted(node), theme, hovered ? control : 'count')
    );
}

/** Left button: the only one a chip answers. */
const PRIMARY_BUTTON = 0;

/** Where a mouse event landed in graph coordinates, asked of the renderer that resolves every pointer
 *  event cytoscape handles itself, so the container's border and the scale the page is drawn at are
 *  accounted for in one place. Not in cytoscape's type definitions, like the drawing hook in
 *  `nodeShadow.ts`; `null` on an instance with no renderer to ask. */
const graphPosition = (cy: Core, event: MouseEvent): { x: number, y: number } | null => {
    const renderer = (cy as unknown as {
        renderer(): { projectIntoViewport?: (clientX: number, clientY: number) => [number, number] }
    }).renderer();
    if (renderer.projectIntoViewport === undefined) {
        return null;
    }
    const [x, y] = renderer.projectIntoViewport(event.clientX, event.clientY);
    return { x, y };
};

/** Make both chips buttons on `cy`. Call once per instance. `theme` is read whenever a chip image is
 *  rewritten, so switching the palette needs nothing here. */
export function installChipButtons(cy: Core, theme: () => DiagramTheme, actions: ChipActions): void {
    const container = cy.container();
    const pointer = (over: boolean) => {
        if (container !== null) {
            container.style.cursor = over ? 'pointer' : '';
        }
    };

    let hovered: NodeSingular | null = null;
    const showCount = () => {
        if (hovered !== null) {
            // A node that a graph update removed while hovered has nothing left to draw.
            if (hovered.inside()) {
                refreshChips(hovered, theme(), false);
            }
            hovered = null;
        }
    };

    cy.on('mouseover', 'node', (event: EventObject) => {
        showCount();
        hovered = event.target as NodeSingular;
        refreshChips(hovered, theme(), true);
    });
    cy.on('mouseout', 'node', () => showCount());
    // A layout moves every node out from under the pointer, and cytoscape only resolves what is hovered
    // on the next pointer move. Without this the control stays on a node the pointer has left, pointing
    // the wrong way, since pressing it is what started the layout.
    cy.on('layoutstop', () => showCount());

    cy.on('mousemove', (event: EventObject) => {
        pointer(chipUnder(cy, event.position.x, event.position.y) !== null);
    });

    const dispatch = (hit: ChipHit) => {
        if (hit.slot === 'code') {
            actions.onSource(hit.node.id());
        } else {
            actions.onToggle(hit.node.id());
        }
    };
    // Touch and pen only: cytoscape routes those through its own touch handling and reports a tap. A
    // mouse press never reaches here, being stopped below before cytoscape sees it.
    cy.on('tap', (event: EventObject) => {
        const hit = chipUnder(cy, event.position.x, event.position.y);
        if (hit !== null) {
            dispatch(hit);
        }
    });

    if (container === null) {
        return;
    }
    const chipUnderMouse = (event: MouseEvent): ChipHit | null => {
        const position = graphPosition(cy, event);
        return position === null ? null : chipUnder(cy, position.x, position.y);
    };
    /** The chip a press is on, until it is let go of. */
    let pressed: ChipHit | null = null;
    // Both bound on the container in the capture phase, ahead of cytoscape's own press handling, which
    // is bound on that same container in the bubble phase.
    container.addEventListener('mousedown', (event) => {
        pressed = event.button === PRIMARY_BUTTON ? chipUnderMouse(event) : null;
        if (pressed === null) {
            return;
        }
        // A press on a button is the button's alone, so cytoscape never sees this one. It hit-tests by
        // the node's own shape, so the press would land on whatever the chip is drawn over as well - a
        // whole region, for a code chip resting in its top band - and cytoscape would paint its press
        // feedback on that, select it, drag it on the smallest movement of the hand, and report a click
        // on it. Which leaves the release below as the one thing that presses the button.
        event.stopPropagation();
        // As cytoscape's own handler does: a press on a canvas must not select the page around it.
        event.preventDefault();
    }, true);
    container.addEventListener('mouseup', (event) => {
        const from = pressed;
        pressed = null;
        if (from === null || event.button !== PRIMARY_BUTTON) {
            return;
        }
        const to = chipUnderMouse(event);
        // Let go of anywhere but the chip it started on: a cancelled press, as with any other button.
        if (to !== null && to.node.id() === from.node.id() && to.slot === from.slot) {
            dispatch(to);
        }
    }, true);
}
