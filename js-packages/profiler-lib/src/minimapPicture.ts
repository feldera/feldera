// Draws the minimap of the diagram: a box for each expanded circuit region, and a dash for each
// operator and collapsed region. The minimap does not show edges.
//
//   Function       Runs                                      Does
//   graphPicture   when a layout is finished                 reads the circuit shape from cytoscape
//   paintPicture   after `graphPicture`, on a theme change   draws the picture

import type { Core } from 'cytoscape';
import { DIAGRAM_PALETTES, REGION_OPACITY, type DiagramTheme } from './diagramTheme.js';
import { Point, Rectangle, Size } from './planar.js';

/** Everything the minimap draws, in model coordinates. */
export interface GraphPicture {
    /** The bounding box of the graph. The minimap shows all of it. */
    box: Rectangle;
    /** Each expanded circuit region. */
    regions: Rectangle[];
    /** The center of each operator and each collapsed circuit region. */
    nodes: Point[];
}

/** The region border width, in minimap pixels. */
const REGION_BORDER_WIDTH = 0.5;
/** The size of an operator dash, in minimap pixels. */
const NODE_THICKNESS = 1;
const NODE_LENGTH = 6 * NODE_THICKNESS;

/** The picture of the current graph, or `null` if the graph is empty. It reads only the values that
 *  the layout sets: the node positions and the region sizes. */
export function graphPicture(cy: Core): GraphPicture | null {
    const box = cy.elements().boundingBox();
    if (!(box.w > 0) || !(box.h > 0)) {
        return null;
    }
    const regions: Rectangle[] = [];
    const nodes: Point[] = [];
    for (const node of cy.nodes().toArray()) {
        // The root node of the circuit is in the graph, but it is never visible.
        if (!node.visible()) {
            continue;
        }
        // Copy the values. `position()` returns the node's own position object, which changes when the
        // node moves.
        const position = node.position();
        const center = new Point(position.x, position.y);
        if (node.isParent()) {
            regions.push(Rectangle.centered(center, new Size(node.outerWidth(), node.outerHeight())));
        } else {
            nodes.push(center);
        }
    }
    return {
        box: new Rectangle(new Point(box.x1, box.y1), new Size(box.w, box.h)),
        regions,
        nodes
    };
}

/** Draw `picture` on `context` at `scale` minimap pixels per model unit. The top left corner of the
 *  picture is at the origin of `context`.
 *
 *  All regions go into one canvas path (a list of shapes that the canvas draws in one call), and all
 *  operators into a second path. So there are only three draw calls (`fill` and `stroke` for the
 *  regions, `stroke` for the operators), for any size of circuit. */
export function paintPicture(
    context: CanvasRenderingContext2D,
    picture: GraphPicture,
    scale: number,
    theme: DiagramTheme
): void {
    const palette = DIAGRAM_PALETTES[theme];
    const origin = picture.box.origin;
    context.save();
    // Draw in model coordinates. The line widths and the dash length are in minimap pixels, so divide
    // them by `scale`.
    context.scale(scale, scale);
    context.translate(-origin.x, -origin.y);

    if (picture.regions.length > 0) {
        context.beginPath();
        for (const region of picture.regions) {
            context.rect(region.origin.x, region.origin.y, region.size.w, region.size.h);
        }
        // Use the region fill of the diagram, so that nested regions look the same as in the diagram.
        context.globalAlpha = REGION_OPACITY;
        context.fillStyle = palette.region;
        context.fill();
        context.globalAlpha = 1;
        context.strokeStyle = palette.border;
        context.lineWidth = REGION_BORDER_WIDTH / scale;
        context.stroke();
    }

    if (picture.nodes.length > 0) {
        const half = NODE_LENGTH / (2 * scale);
        context.beginPath();
        for (const node of picture.nodes) {
            context.moveTo(node.x - half, node.y);
            context.lineTo(node.x + half, node.y);
        }
        // `navigatorInk` is gray: dark enough to see, but light enough that the viewport outline stays
        // visible on a dense minimap.
        context.strokeStyle = palette.navigatorInk;
        context.lineWidth = NODE_THICKNESS / scale;
        context.stroke();
    }
    context.restore();
}
