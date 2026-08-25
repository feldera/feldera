// The accent glow marking the node whose metrics are on display. Nothing else is painted around a
// node: an operator is told from the canvas by its own border.
//
// Cytoscape has no shadow style property. Its closest offer, `underlay`, is a hard-edged filled shape,
// which reads as a second border rather than as a glow. So the glow is painted with the canvas' own
// shadow support, by wrapping the one renderer entry point that draws beneath a node body:
// `drawNodeUnderlay`. That hook runs with the context already in graph coordinates, beneath the node body
// and its chips, and outside the cached element texture, so a glow reaching past the node's bounding
// box is not clipped.

import type { Core, NodeSingular } from 'cytoscape';

/** Marks the node whose metrics are on display, whether it was clicked, hovered or searched for.
 *  Named apart from cytoscape's own `:selected` state, which the diagram does not use. */
export const SELECTED_NODE_CLASS = 'selected-node';

/** A canvas shadow, in graph units: the blur radius and the offset of the shadow from the shape
 *  casting it. */
export interface NodeShadow {
    /** Shadow color, its alpha included. */
    color: string;
    blur: number;
    offsetX: number;
    offsetY: number;
}

/** Accent glow on the selected node: secondary-500 of the Feldera theme, `oklch(81.09% 0.14 69.14deg)`
 *  converted to sRGB. Centered rather than cast, so it reads as the node being lit. The same color in
 *  either palette. */
export const SELECTION_GLOW: NodeShadow = {
    color: 'rgba(251, 175, 81, 1)',
    blur: 20,
    offsetX: 0,
    offsetY: 0
};

/** The glow around `node`, or `null` for every node but the marked one. An expanded region never
 *  glows: the glow would run along the inside of its border and read as a border of its own. */
export function nodeShadow(node: NodeSingular): NodeShadow | null {
    return !node.isParent() && node.hasClass(SELECTED_NODE_CLASS) ? SELECTION_GLOW : null;
}

/** How far the glow reaches past the node it surrounds, in graph units. */
export const shadowReach = (s: NodeShadow): number =>
    s.blur + Math.max(Math.abs(s.offsetX), Math.abs(s.offsetY));

/** Distance the shape is moved out of frame so that only its shadow lands on the canvas. Large
 *  enough that no node's shape can be dragged back into view by its own extent. */
const OFF_CANVAS = 1e6;

/** Paint the glow around `node`, and nothing else. `context` is in graph coordinates, `pos` is the
 *  node's center and `w`/`h` its body box - the same values cytoscape draws the body with. */
function paintShadow(
    context: CanvasRenderingContext2D,
    node: NodeSingular,
    shadow: NodeShadow,
    pos: { x: number, y: number },
    w: number,
    h: number
): void {
    // Canvas shadow offsets and blur are in device pixels: unlike the path, they are not put through
    // the context transform. So they have to be scaled by whatever transform is in effect, which is
    // the viewport zoom when drawing to the screen and the texture scale when drawing to a cache.
    const scale = context.getTransform().a;
    const radius = Number.parseFloat(node.style('corner-radius')) || 0;

    context.save();
    context.fillStyle = shadow.color;
    context.shadowColor = shadow.color;
    context.shadowBlur = shadow.blur * scale;
    // The shape is drawn a long way off to the left and its shadow offset by as much to the right,
    // which leaves the blurred shadow in place of the shape and the shape itself out of frame. A
    // plain fill would otherwise cover the node's own box with an opaque rectangle.
    context.shadowOffsetX = (OFF_CANVAS + shadow.offsetX) * scale;
    context.shadowOffsetY = shadow.offsetY * scale;
    context.beginPath();
    context.roundRect(pos.x - OFF_CANVAS - w / 2, pos.y - h / 2, w, h, radius);
    context.fill();
    context.restore();
}

/** Renderer entry point wrapped by `installNodeShadows`: `(context, node, pos?, w?, h?)`, called
 *  once per node per frame, from inside the node's own draw pass. */
type DrawNodeUnderlay = (
    context: CanvasRenderingContext2D,
    node: NodeSingular,
    pos?: { x: number, y: number },
    w?: number,
    h?: number
) => void;

/** Start painting the selection glow on `cy`. Call once per instance, after construction. */
export function installNodeShadows(cy: Core): void {
    // `renderer()` and its draw methods are not part of cytoscape's documented API. Only the first two
    // arguments are read and the rest passed straight through, so a renderer that adds or reorders
    // arguments still works.
    const renderer = (cy as unknown as { renderer(): Record<string, unknown> }).renderer();
    const original = renderer['drawNodeUnderlay'] as DrawNodeUnderlay | undefined;
    if (typeof original !== 'function') {
        // A headless instance has no canvas renderer, and so nothing to draw the glow on.
        return;
    }
    renderer['drawNodeUnderlay'] = function (
        this: unknown,
        context: CanvasRenderingContext2D,
        node: NodeSingular,
        ...rest: [({ x: number, y: number } | undefined)?, (number | undefined)?, (number | undefined)?]
    ): void {
        const shadow = node.visible() ? nodeShadow(node) : null;
        if (shadow !== null) {
            const [pos, w, h] = rest;
            // The body box cytoscape draws, which is the node's own size plus its padding. The
            // border straddles the edge of that box and so adds nothing to it.
            const padding = Number(node.numericStyle('padding')) || 0;
            paintShadow(
                context,
                node,
                shadow,
                pos ?? node.position(),
                w ?? node.width() + 2 * padding,
                h ?? node.height() + 2 * padding
            );
        }
        original.call(this, context, node, ...rest);
    } as DrawNodeUnderlay;
}
