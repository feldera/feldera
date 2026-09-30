// Paints an accent glow around the node whose metrics are shown. Other nodes have no glow or shadow:
// the border of an operator is enough to show it on the canvas.
//
// Cytoscape has no shadow style. The `underlay` style draws a filled shape with hard edges, which
// looks like a second border, not a glow. So this file paints the glow with the canvas shadow API.
// It paints in the `drawNodeUnderlay` layer of `cytoscapeRenderer.ts`, below the node body and its
// chips. That layer is not part of the cached image of the node, so a glow that is larger than the
// bounding box of the node is not clipped.

import type { Core, NodeSingular } from 'cytoscape';
import { paintNodeLayer } from './cytoscapeRenderer.js';

/** The class of the node whose metrics are shown: the node that was clicked, hovered or found by a
 *  search. The name is not `selected`, so that it is not confused with the cytoscape `:selected`
 *  state, which the diagram does not use. */
export const SELECTED_NODE_CLASS = 'selected-node';

/** A canvas shadow, in graph units. `blur` is the blur radius. `offsetX` and `offsetY` move the shadow
 *  away from its shape. */
export interface NodeShadow {
    /** The shadow color, with alpha. */
    color: string;
    blur: number;
    offsetX: number;
    offsetY: number;
}

/** The glow of the selected node. The color is secondary-500 of the Feldera theme,
 *  `oklch(81.09% 0.14 69.14deg)`, in sRGB. Both palettes use it. The offset is 0, so the glow is the
 *  same on all sides of the node. */
export const SELECTION_GLOW: NodeShadow = {
    color: 'rgba(251, 175, 81, 1)',
    blur: 20,
    offsetX: 0,
    offsetY: 0
};

/** The glow of `node`, or `null` if `node` is not the selected node. An expanded region has no glow,
 *  because the glow would show along the inside of its border and look like a second border. */
export function nodeShadow(node: NodeSingular): NodeShadow | null {
    return !node.isParent() && node.hasClass(SELECTED_NODE_CLASS) ? SELECTION_GLOW : null;
}

/** How far the glow extends past the node, in graph units. */
export const shadowReach = (s: NodeShadow): number =>
    s.blur + Math.max(Math.abs(s.offsetX), Math.abs(s.offsetY));

/** The distance by which `paintShadow` moves the box out of view, in graph units. It is much larger
 *  than any diagram, so the box is never in view. */
const OFF_CANVAS = 1e6;

/** Paint `shadow` for a `w` by `h` box centered on `pos`, with the corner radius of `node`. Only the
 *  shadow is painted, not the box. `context` is in graph coordinates. */
function paintShadow(
    context: CanvasRenderingContext2D,
    node: NodeSingular,
    shadow: NodeShadow,
    pos: { x: number, y: number },
    w: number,
    h: number
): void {
    // The canvas does not apply its transform to the shadow offset and blur, so they are in device
    // pixels. Scale them by the current transform: the viewport zoom when cytoscape draws to the
    // screen, or the scale of the cached image when it draws to a cache.
    const scale = context.getTransform().a;
    const radius = Number.parseFloat(node.style('corner-radius')) || 0;

    context.save();
    context.fillStyle = shadow.color;
    context.shadowColor = shadow.color;
    context.shadowBlur = shadow.blur * scale;
    // Draw the box `OFF_CANVAS` to the left, and move its shadow by the same distance to the right.
    // The shadow is then around the node, and the box is out of view. A box in view would cover the
    // node with an opaque fill.
    context.shadowOffsetX = (OFF_CANVAS + shadow.offsetX) * scale;
    context.shadowOffsetY = shadow.offsetY * scale;
    context.beginPath();
    context.roundRect(pos.x - OFF_CANVAS - w / 2, pos.y - h / 2, w, h, radius);
    context.fill();
    context.restore();
}

/** Start to paint the selection glow on `cy`. Call this one time for each cytoscape instance, after
 *  you create it. */
export function installNodeShadows(cy: Core): void {
    paintNodeLayer(cy, 'drawNodeUnderlay', (context, node, body) => {
        const shadow = nodeShadow(node);
        if (shadow !== null) {
            paintShadow(context, node, shadow, body.center, body.width, body.height);
        }
    });
}
