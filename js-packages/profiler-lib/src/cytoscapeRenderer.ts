// Typed access to the cytoscape canvas renderer. `cy.renderer()` and its functions are not in the
// cytoscape documentation or type definitions, so a cytoscape update can change them. All code that
// uses them is in this file.
//
//   Function              Used for
//
//   projectIntoViewport   convert a mouse position to graph coordinates (`chipButtons.ts`)
//
//   drawNodeUnderlay      paint below the node body and its chips (`nodeShadow.ts`)
//
//   drawNodeOverlay       paint above the node body and its chips (`nodeText.ts`)

import type { Core, NodeSingular } from 'cytoscape';

type Point = { x: number, y: number };

/** The type of `drawNodeUnderlay` and `drawNodeOverlay`. Cytoscape calls them one time for each node
 *  in each frame, with `context` in graph coordinates. `pos`, `w` and `h` are the center and size of
 *  the node body, and can be missing. */
type DrawNodeLayer = (
    context: CanvasRenderingContext2D,
    node: NodeSingular,
    pos?: Point,
    w?: number,
    h?: number
) => void;

/** The renderer functions that this file uses. A headless cytoscape instance has none of them. */
interface CanvasRenderer {
    projectIntoViewport?: (clientX: number, clientY: number) => [number, number];
    drawNodeUnderlay?: DrawNodeLayer;
    drawNodeOverlay?: DrawNodeLayer;
}

const canvasRenderer = (cy: Core): CanvasRenderer =>
    (cy as unknown as { renderer(): CanvasRenderer }).renderer();

/** The position of a mouse event in graph coordinates, or `null` if `cy` has no canvas renderer. The
 *  renderer includes the container border and the page zoom in the conversion. */
export function projectIntoViewport(cy: Core, event: MouseEvent): Point | null {
    const renderer = canvasRenderer(cy);
    if (renderer.projectIntoViewport === undefined) {
        return null;
    }
    const [x, y] = renderer.projectIntoViewport(event.clientX, event.clientY);
    return { x, y };
}

/** The center and size of a node body, in graph coordinates. */
export interface NodeBody {
    center: Point;
    width: number;
    height: number;
}

/** Call `paint` for each visible node in each frame, below or above the node body and its chips.
 *  Call this one time for each cytoscape instance, after you create it. If `cy` has no canvas
 *  renderer, this does nothing. */
export function paintNodeLayer(
    cy: Core,
    layer: 'drawNodeUnderlay' | 'drawNodeOverlay',
    paint: (context: CanvasRenderingContext2D, node: NodeSingular, body: NodeBody) => void
): void {
    const renderer = canvasRenderer(cy);
    const original = renderer[layer];
    if (typeof original !== 'function') {
        return;
    }
    // Pass all arguments to the original function unchanged, so that it works also if a cytoscape
    // update adds arguments.
    renderer[layer] = function (
        this: unknown,
        context: CanvasRenderingContext2D,
        node: NodeSingular,
        ...rest: [Point?, number?, number?]
    ): void {
        if (node.visible()) {
            const [pos, w, h] = rest;
            // The body is the node size plus its padding. The border is centered on the edge of the
            // body, so it does not make the body larger.
            const padding = Number(node.numericStyle('padding')) || 0;
            paint(context, node, {
                center: pos ?? node.position(),
                width: w ?? node.width() + 2 * padding,
                height: h ?? node.height() + 2 * padding
            });
        }
        original.call(this, context, node, ...rest);
    };
}
