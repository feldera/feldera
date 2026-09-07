// What the minimap shows: a box per region, a dash per operator, a line per edge when `DRAW_EDGES` is
// on. Nothing else - at a hundred pixels across an arrowhead is one pixel the color of its own line, and
// a label is a smudge.
//
// `graphPicture` reads the shape of the circuit out of cytoscape, `paintPicture` draws it. Two steps,
// because the reading happens once a layout has settled and the drawing again on every palette change.
// Only what the layout decided is read: where every node landed, and how big the regions came out. When
// the picture is taken the renderer has drawn nothing yet, so it has no answer for where an edge runs.
//
// The whole picture rasterizes in four canvas calls, whatever the size of the circuit: regions are one
// path, edges one, operators one. That is what keeps a big one off the frame budget.

import type { Core } from 'cytoscape';
import { DIAGRAM_PALETTES, REGION_OPACITY, type DiagramTheme } from './diagramTheme.js';
import { Point, Rectangle, Size } from './planar.js';

/** A box in model coordinates, from its top left corner. */
export interface PictureBox {
    x: number;
    y: number;
    w: number;
    h: number;
}

/** A point in model coordinates. */
export interface PicturePoint {
    x: number;
    y: number;
}

/** An edge, as a straight line. */
export interface PictureLine {
    x1: number;
    y1: number;
    x2: number;
    y2: number;
}

/** Everything the minimap draws, in model coordinates. */
export interface GraphPicture {
    /** The graph's bounding box, which is the whole of the map. */
    box: Rectangle;
    regions: PictureBox[];
    /** Center of every operator, and of every composite that is collapsed. */
    nodes: PicturePoint[];
    /** One line per edge, center to center. */
    edges: PictureLine[];
}

/** Whether the lines between the operators are drawn. While off they are not even collected: at the size
 *  of the map the lines of a real circuit crowd the dots that say where its operators are. */
export const DRAW_EDGES = false;

/** Edge width, in map pixels: a hairline at any scale. */
const EDGE_WIDTH = 1;
/** Region border, thinner: the shape inside it is tinted already. */
const REGION_BORDER_WIDTH = 0.5;
/** The dash an operator is drawn as, in map pixels. One size for all of them,
 * so their own widths and heights are never read - a circuit big enough for the difference to
 *  show is one where an operator is under a pixel tall anyway. */
const NODE_THICKNESS = 1;
const NODE_LENGTH = 6 * NODE_THICKNESS;

/** The shape of the circuit as it now stands, or `null` when there is nothing on the graph. */
export function graphPicture(cy: Core): GraphPicture | null {
    const box = cy.elements().boundingBox();
    if (!(box.w > 0) || !(box.h > 0)) {
        return null;
    }
    const regions: PictureBox[] = [];
    const nodes: PicturePoint[] = [];
    for (const node of cy.nodes().toArray()) {
        // The circuit's root node is on the graph but never drawn.
        if (!node.visible()) {
            continue;
        }
        const position = node.position();
        if (node.isParent()) {
            const w = node.outerWidth();
            const h = node.outerHeight();
            regions.push({ x: position.x - w / 2, y: position.y - h / 2, w, h });
        } else {
            // Copied out: cytoscape hands back the node's own position object.
            nodes.push({ x: position.x, y: position.y });
        }
    }
    // Straight from node to node. Where an edge really runs is the renderer's to say and it has drawn
    // nothing yet; at a hundred pixels a taxi route and its straight line are a pixel or two apart.
    const edges = !DRAW_EDGES ? [] : cy.edges().toArray().filter((edge) => edge.visible()).map((edge) => {
        const from = edge.source().position();
        const to = edge.target().position();
        return { x1: from.x, y1: from.y, x2: to.x, y2: to.y };
    });
    return {
        box: new Rectangle(new Point(box.x1, box.y1), new Size(box.w, box.h)),
        regions,
        nodes,
        edges
    };
}

/** Paint `picture` at `scale` map pixels per model unit, its top left corner on the origin of
 *  `context`. */
export function paintPicture(
    context: CanvasRenderingContext2D,
    picture: GraphPicture,
    scale: number,
    theme: DiagramTheme
): void {
    const palette = DIAGRAM_PALETTES[theme];
    const origin = picture.box.origin;
    context.save();
    // Drawn in model coordinates. The widths and the size of a dash are in map pixels, hence divided by
    // the scale wherever they are used.
    context.scale(scale, scale);
    context.translate(-origin.x, -origin.y);

    if (picture.regions.length > 0) {
        context.beginPath();
        for (const region of picture.regions) {
            context.rect(region.x, region.y, region.w, region.h);
        }
        // The tint the diagram fills a region with, so nesting reads the same way.
        context.globalAlpha = REGION_OPACITY;
        context.fillStyle = palette.region;
        context.fill();
        context.globalAlpha = 1;
        context.strokeStyle = palette.border;
        context.lineWidth = REGION_BORDER_WIDTH / scale;
        context.stroke();
    }

    if (picture.edges.length > 0) {
        context.beginPath();
        for (const line of picture.edges) {
            context.moveTo(line.x1, line.y1);
            context.lineTo(line.x2, line.y2);
        }
        context.strokeStyle = palette.navigatorInk;
        context.lineWidth = EDGE_WIDTH / scale;
        context.stroke();
    }

    if (picture.nodes.length > 0) {
        const half = NODE_LENGTH / (2 * scale);
        context.beginPath();
        for (const node of picture.nodes) {
            context.moveTo(node.x - half, node.y);
            context.lineTo(node.x + half, node.y);
        }
        // Gray: dark enough to see, light enough for the viewport outline to show over a dense map.
        context.strokeStyle = palette.navigatorInk;
        context.lineWidth = NODE_THICKNESS / scale;
        context.stroke();
    }
    context.restore();
}
