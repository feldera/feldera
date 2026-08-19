// Node shadows: the ambient shadow that lifts every non-expanded node off the canvas, and the
// accent glow marking the selected one.
//
// Cytoscape has no shadow style property. Its closest offer, `underlay`, is a hard-edged filled shape,
// which reads as a second border rather than as a shadow. So shadows are painted with the canvas' own
// shadow support, by wrapping the one renderer entry point that draws beneath a node body:
// `drawNodeUnderlay`. That hook runs with the context already in graph coordinates, beneath the node body
// and its chips, and outside the cached element texture, so a shadow reaching past the node's bounding
// box is not clipped.

import type { Core, NodeSingular } from 'cytoscape';
import { DIAGRAM_PALETTES, type DiagramTheme, NO_SHADOW } from './diagramTheme.js';

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

/** Ambient shadow: cast down and to the right, as if the diagram were lit from the top left. The palette
 *  decides its color, and whether there is one at all - `NO_SHADOW` becomes `null` here, and nothing
 *  painted. */
const AMBIENT_CAST = { blur: 3, offsetX: 1.5, offsetY: 2 };
const ambient = (theme: DiagramTheme): NodeShadow | null => {
    const color = DIAGRAM_PALETTES[theme].shadow;
    return color === NO_SHADOW ? null : { color, ...AMBIENT_CAST };
};
export const AMBIENT_SHADOW: Record<DiagramTheme, NodeShadow | null> = {
    light: ambient('light'),
    dark: ambient('dark')
};

/** Accent glow on the selected node: secondary-500 of the Feldera theme, `oklch(81.09% 0.14 69.14deg)`
 *  converted to sRGB. Centered rather than cast, so it reads as the node being lit rather than as a
 *  heavier shadow, and wider than the ambient one. The same color in either palette. */
export const SELECTION_GLOW: NodeShadow = {
    color: 'rgba(251, 175, 81, 1)',
    blur: 20,
    offsetX: 0,
    offsetY: 0
};

/** The shadow a node casts, or `null` when it casts none: an expanded region in any palette, and every
 *  node of a palette that declines the ambient shadow. The accent glow is not the palette's to
 *  decline. */
export function nodeShadow(node: NodeSingular, theme: DiagramTheme): NodeShadow | null {
    // An expanded region holds nodes that cast their own shadows, and one on the region too would
    // double up along every inner edge. Collapsed it has no children, and casts a shadow like any
    // other node.
    if (node.isParent()) {
        return null;
    }
    return node.hasClass(SELECTED_NODE_CLASS) ? SELECTION_GLOW : AMBIENT_SHADOW[theme];
}

/** How far a shadow reaches past the shape casting it, in graph units. */
export const shadowReach = (s: NodeShadow): number =>
    s.blur + Math.max(Math.abs(s.offsetX), Math.abs(s.offsetY));

/** Distance the shape is moved out of frame so that only its shadow lands on the canvas. Large
 *  enough that no node's shape can be dragged back into view by its own extent. */
const OFF_CANVAS = 1e6;

/** Paint the shadow of `node`, and nothing else. `context` is in graph coordinates, `pos` is the
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

/** Start painting node shadows on `cy`. Call once per instance, after construction. `theme` is read
 *  on every frame, so switching the palette needs no more than the repaint it already does. */
export function installNodeShadows(cy: Core, theme: () => DiagramTheme): void {
    // `renderer()` and its draw methods are not part of cytoscape's documented API. Only the first two
    // arguments are read and the rest passed straight through, so a renderer that adds or reorders
    // arguments still works.
    const renderer = (cy as unknown as { renderer(): Record<string, unknown> }).renderer();
    const original = renderer['drawNodeUnderlay'] as DrawNodeUnderlay | undefined;
    if (typeof original !== 'function') {
        // A headless instance has no canvas renderer, and so nothing to draw shadows on.
        return;
    }
    renderer['drawNodeUnderlay'] = function (
        this: unknown,
        context: CanvasRenderingContext2D,
        node: NodeSingular,
        ...rest: [({ x: number, y: number } | undefined)?, (number | undefined)?, (number | undefined)?]
    ): void {
        const shadow = node.visible() ? nodeShadow(node, theme()) : null;
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
