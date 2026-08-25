// Makes the two chips on a node work as buttons.
//
//   Chip      Shows                                      When clicked
//
//   code      that the node has SQL source               show the SQL source of the node
//
//   counter   the number of operators in the region;     expand or collapse the circuit region
//             on hover, an expand or collapse icon
//
// Every circuit region can be expanded or collapsed, nested or not.
//
// Chips are cytoscape background images. Cytoscape's default behavior of finding the node
// under the pointer cannot be used because it depends on the pointer hit testing the node's shape,
// and the code chip is above the node's top edge. So this file calculates the box of each
// chip, finds the chip under the pointer and the node it belongs to, and handles the clicks:
//
//   Input          What happens
//
//   mouse          this file hides `mousedown` from cytoscape, and clicks the chip on `mouseup`
//
//   touch or pen   cytoscape reports a tap, and this file clicks the chip under it

import type { Core, EventObject, NodeSingular } from 'cytoscape';
import {
    badgePillWidth,
    BADGE_CANVAS_WIDTH,
    CHIP_NONE,
    CODE_CHIP_WIDTH,
    formatLeafCount,
    nodeChips
} from './chips.js';
import { projectIntoViewport } from './cytoscapeRenderer.js';
import type { DiagramTheme } from './diagramTheme.js';

/** A slot is a fixed place for one chip. Each node has two slots, one for each background image in
 *  the chip style (`chips.ts`). Each background image style (`background-image`, `background-width`,
 *  and so on) holds a list with one value per slot: `code` is index 0, `counter` is index 1. A slot
 *  exists on every node, but it can be empty (`CHIP_NONE`), for example on an operator without SQL
 *  source. */
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

/** What a chip does when clicked. */
export interface ChipActions {
    /** The code chip: show the SQL source of the node it sits on. */
    onSource: (nodeId: string) => void;
    /** The counter chip: expand or collapse the circuit region it counts. */
    onToggle: (nodeId: string) => void;
}

/** Value `index` of a style that has one value for each background image. Cytoscape returns these
 *  values joined by spaces. */
const slotValue = (node: NodeSingular, property: string, index: number): string =>
    String(node.style(property)).split(' ')[index] ?? '';

const pixels = (value: string): number => Number.parseFloat(value) || 0;

/** The distance from the left edge of the node body to the left edge of the image (or top to top, for
 *  the y axis). It is called individually for each axis.
 *  This function repeats the same arithmetic Cytoscape does when placing the chip images
 *  based on user-provided stylesheet. The value is derived as a sum of the two styles:
 * `background-position` and `background-offset`. In the style, a pixel value is a distance,
 *  a percent value is a part of the space that is left over next to the image (`boxSize - imageSize`):
 *  on the x axis, 0% puts the image at the left edge of the body, 100% at the right edge. For example,
 *  the chips use `100%` and `-CHIP_INSET` px, which puts the right edge of the image `CHIP_INSET`
 *  in from the right edge of the body. */
const imageInset = (boxSize: number, imageSize: number, position: string, offset: string): number => {
    const distance = (value: string) =>
        value.endsWith('%') ? ((boxSize - imageSize) * pixels(value)) / 100 : pixels(value);
    return distance(position) + distance(offset);
};

/** The number of operators inside `node`, or 0. */
const leafCount = (node: NodeSingular): number => Number(node.data('leaf_count')) || 0;

/** True if `node` is a circuit region, nested or not. Only these can be expanded and collapsed. */
export const isToggleable = (node: NodeSingular): boolean => Boolean(node.data('has_children'));

/** The width of the visible pill of a chip. The counter image is wide enough for the longest count,
 *  but its pill is only as wide as the count it shows. */
const pillWidth = (node: NodeSingular, slot: ChipSlot): number =>
    slot === 'code' ? CODE_CHIP_WIDTH : badgePillWidth(formatLeafCount(leafCount(node)));

/** The box of one chip of `node`, or `null` if the node does not show that chip. */
export function chipBox(node: NodeSingular, slot: ChipSlot): ChipBox | null {
    const index = SLOT_INDEX[slot]!;
    if (slotValue(node, 'background-image', index) === CHIP_NONE) {
        return null;
    }
    // Cytoscape places background images relative to the node body: the node size plus padding.
    const padding = Number(node.numericStyle('padding')) || 0;
    const bodyWidth = node.width() + 2 * padding;
    const bodyHeight = node.height() + 2 * padding;
    const canvasWidth = pixels(slotValue(node, 'background-width', index));
    const canvasHeight = pixels(slotValue(node, 'background-height', index));
    const position = node.position();
    const left = position.x - bodyWidth / 2
        + imageInset(bodyWidth, canvasWidth,
            slotValue(node, 'background-position-x', index),
            slotValue(node, 'background-offset-x', index));
    const top = position.y - bodyHeight / 2
        + imageInset(bodyHeight, canvasHeight,
            slotValue(node, 'background-position-y', index),
            slotValue(node, 'background-offset-y', index));
    // The pill is at the right of its image. The rest of the image is transparent and is not part of
    // the button.
    const right = left + canvasWidth;
    return { x1: right - pillWidth(node, slot), y1: top, x2: right, y2: top + canvasHeight };
}

/** True if `node` is displayed. Cytoscape's `visible()` is also false for a node with zero width, and
 *  a node that takes its width from its label has zero width until the label is measured. */
const shown = (node: NodeSingular): boolean =>
    String(node.style('display')) !== 'none' && String(node.style('visibility')) === 'visible';

const containsPoint = (box: ChipBox, x: number, y: number): boolean =>
    x >= box.x1 && x <= box.x2 && y >= box.y1 && y <= box.y2;

/** The chip of `node` at a point in graph coordinates, or `null`. */
export function chipAt(node: NodeSingular, x: number, y: number): ChipSlot | null {
    for (const slot of SLOTS) {
        const box = chipBox(node, slot);
        if (box !== null && containsPoint(box, x, y)) {
            return slot;
        }
    }
    return null;
}

/** The chip at a point in graph coordinates, on any node. A fast bounding box check skips most nodes.
 *  The box is made wider on the left, because a chip can be wider than its node. If the chip of a node
 *  and the chip of its region overlap, the node wins, because it is drawn on top. */
export function hitTestChips(cy: Core, x: number, y: number): ChipHit | null {
    let region: ChipHit | null = null;
    for (const node of cy.nodes().toArray()) {
        const box = node.boundingBox();
        if (!shown(node)
            || x < box.x1 - BADGE_CANVAS_WIDTH || x > box.x2 || y < box.y1 || y > box.y2) {
            continue;
        }
        const slot = chipAt(node, x, y);
        if (slot === null || (slot === 'counter' && !isToggleable(node))) {
            continue;
        }
        if (!node.isParent()) {
            return { node, slot };
        }
        region ??= { node, slot };
    }
    return region;
}

/** Update the chip images of `node` for `theme`. Also call this after a theme change. When the pointer
 *  is on a top-level region (`hovered`), its counter shows an icon instead of the count: a square to
 *  expand a collapsed region, or a dash to collapse an expanded region. */
export function refreshChips(node: NodeSingular, theme: DiagramTheme, hovered = false): void {
    const control = node.isParent() ? 'collapse' : 'expand';
    const glyph = hovered && isToggleable(node) ? control : 'count';
    node.data(
        'chips',
        nodeChips(Boolean(node.data('has_source')), leafCount(node), theme, glyph)
    );
}

/** The left mouse button. Chips ignore the other buttons. */
const PRIMARY_BUTTON = 0;

/** Make the chips on `cy` work as buttons. Call this one time for each cytoscape instance. */
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
            // A graph update can remove the node under the pointer. A removed node has no chips.
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
    // A layout moves the nodes, but cytoscape finds the node under the pointer again only when the
    // pointer moves. Without this, the clicked region keeps its icon, which now shows the wrong action.
    cy.on('layoutstop', () => showCount());

    cy.on('mousemove', (event: EventObject) => {
        pointer(hitTestChips(cy, event.position.x, event.position.y) !== null);
    });

    const dispatch = (hit: ChipHit) => {
        if (hit.slot === 'code') {
            actions.onSource(hit.node.id());
        } else {
            actions.onToggle(hit.node.id());
        }
    };
    // Touch and pen input only. A mouse click does not get here, because the `mousedown` listener below
    // stops it.
    cy.on('tap', (event: EventObject) => {
        const hit = hitTestChips(cy, event.position.x, event.position.y);
        if (hit !== null) {
            dispatch(hit);
        }
    });

    if (container === null) {
        return;
    }
    const chipAtMouse = (event: MouseEvent): ChipHit | null => {
        const point = projectIntoViewport(cy, event);
        return point === null ? null : hitTestChips(cy, point.x, point.y);
    };
    /** The chip under the pointer at `mousedown`, until `mouseup`. */
    let pressed: ChipHit | null = null;
    // Both listeners use the capture phase, so they run before the cytoscape listeners on the
    // container.
    container.addEventListener('mousedown', (event) => {
        pressed = event.button === PRIMARY_BUTTON ? chipAtMouse(event) : null;
        if (pressed === null) {
            return;
        }
        // Hide `mousedown` from cytoscape. Otherwise cytoscape sees a mouse click on the node
        // under the chip, for example a whole region under a code chip. It would then select, drag and
        // click that node.
        event.stopPropagation();
        // Do not select text on the page, as the cytoscape `mousedown` handler also does.
        event.preventDefault();
    }, true);
    container.addEventListener('mouseup', (event) => {
        const from = pressed;
        pressed = null;
        if (from === null || event.button !== PRIMARY_BUTTON) {
            return;
        }
        const to = chipAtMouse(event);
        // As with any button, a `mouseup` outside the chip of the `mousedown` cancels the click.
        if (to !== null && to.node.id() === from.node.id() && to.slot === from.slot) {
            dispatch(to);
        }
    }, true);
}
