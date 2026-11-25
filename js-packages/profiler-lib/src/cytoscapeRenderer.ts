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
//
//   requestedFrame        stop the frames while a layout runs (`cytograph.ts`)

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
    requestedFrame?: boolean;
    redraw?: () => void;
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

/** Stops cytoscape from drawing the diagram until the returned function is called. Does nothing if
 *  `cy` has no canvas renderer.
 *
 *  What this prevents: when a region is expanded or collapsed, the graph changes at once, but ELK
 *  gives the new positions only some animation frames later. Without the pause, cytoscape draws the
 *  changed graph at the old positions in those frames: a style update of all changed elements and a
 *  full draw. The user does not see these frames, because `FrozenLayout` shows a picture of the old
 *  diagram over them. On a large bundle they cost about 0.8 s. With the pause, cytoscape draws only
 *  the finished layout.
 *
 *  How: `requestedFrame` is a private field of the cytoscape renderer, not a public API. Cytoscape
 *  sets it to true when the diagram must be drawn again. In each animation frame, the render loop of
 *  cytoscape 3.35 draws only if the field is true, and sets it to false. The pause makes the field
 *  always read false, and keeps what cytoscape writes to it. The returned function puts back the last
 *  written value, so the next frame draws all the changes. `diagramFrames.test.ts` in
 *  profiler-layout fails if a cytoscape update changes this loop.
 *
 *  The public `cy.startBatch()` also stops the frames, but not only the frames: after two overlapping
 *  layouts in a batch, the nodes do not get the positions that ELK gives without the batch. */
export function pauseFrames(cy: Core): () => void {
    const renderer = canvasRenderer(cy);
    if (typeof renderer.redraw !== 'function') {
        return () => { };
    }
    let isFrameRequested = renderer.requestedFrame === true;
    // Regression tests: "draws no frame until the layout is finished, then draws the new layout" and
    // "draws no frame until the last of two overlapping layouts is finished" in
    // `profiler-layout/src/lib/components/diagramFrames.test.ts`.
    Object.defineProperty(renderer, 'requestedFrame', {
        configurable: true,
        get: () => false,
        set: (value: boolean) => {
            isFrameRequested = value;
        }
    });
    return () => {
        // Remove the accessor, and put back a plain property with the kept request.
        delete renderer.requestedFrame;
        renderer.requestedFrame = isFrameRequested;
    };
}
