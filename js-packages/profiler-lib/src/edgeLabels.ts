// Finds the edge label under the pointer. Edges ignore pointer events (`events: 'no'` in
// `diagramTheme.ts`), so cytoscape cannot report a label under the pointer. This file tests the
// pointer against the label boxes instead.

import type { BoundingBox12, Core, EdgeSingular } from 'cytoscape';

/** The box of the label chip of `edge`, in graph coordinates. Cytoscape includes the padding and the
 *  border of the chip in the box. */
const labelBox = (edge: EdgeSingular): BoundingBox12 =>
    edge.boundingBox({
        includeNodes: false,
        includeEdges: false,
        includeLabels: true,
        includeMainLabels: true,
        includeSourceLabels: false,
        includeTargetLabels: false,
        includeOverlays: false
    });

interface LabelBox {
    edge: EdgeSingular;
    box: BoundingBox12;
}

/** Keeps the label boxes of all labeled edges until cytoscape reports a change. The hit test runs on
 *  each pointer move, so it must not read the box of each edge from cytoscape each time. */
class LabelBoxCache {
    private boxes: LabelBox[] | null = null;

    constructor(private readonly cy: Core) {
        // A change of the graph or of edge data can add or remove a label. Cytoscape emits `bounds` when
        // it applies a style that changes the box of an edge, for example when a hover traces the edge.
        // The next pointer move then reads all boxes again: about 0.1 ms for 250 labeled edges, and
        // 30 ms for 20000. A hover change of 20000 edges takes cytoscape more than 0.5 s, so the cache
        // does not track the box of each edge.
        cy.on('add remove data bounds', 'edge', () => {
            this.boxes = null;
        });
    }

    /** The labeled edges, and the box of the label chip of each one in graph coordinates. */
    labelBoxes(): LabelBox[] {
        this.boxes ??= this.cy.edges('[label]').map((edge) => ({ edge, box: labelBox(edge) }));
        return this.boxes;
    }
}

/** Finds the edge label under a point. */
export class EdgeLabelFinder {
    private readonly cache: LabelBoxCache;

    constructor(cy: Core) {
        this.cache = new LabelBoxCache(cy);
    }

    /** An edge whose label contains the point (`x`, `y`) in graph coordinates, or null. */
    edgeAt(x: number, y: number): EdgeSingular | null {
        const hit = this.cache.labelBoxes().find(({ box }) =>
            box.x1 <= x && x <= box.x2 && box.y1 <= y && y <= box.y2);
        return hit?.edge ?? null;
    }
}
