// How wide an expanded region has to be to fit its own name.
//
// Cytoscape sizes a parent node from the nodes it contains and ignores the parent's own label. A region
// that holds a single narrow operator therefore comes out narrower than the name drawn in the band along
// its top edge, so the name overflows both borders and runs under the counter chip in the corner.
//
// Two systems have to use the same width. Cytoscape draws the region box, and ELK spaces the region's
// siblings by the size it computes for the region. Widening the region in cytoscape alone makes it
// overlap its neighbour; widening it in ELK alone leaves a gap that the region never fills. So the width
// is computed once here, and returned in the two forms the two systems accept.

import { badgePillWidth, CHIP_INSET, formatLeafCount } from './chips.js';
import { labelWidth, REGION_PADDING } from './diagramTheme.js';

/** Horizontal space, in pixels, left between the region's name and the counter chip next to it, so that
 *  the name does not run up against the chip. */
const NAME_CHIP_GAP = 6;

/** ELK's default `elk.padding`, in pixels: the space it leaves between a region's border and the nodes
 *  inside it. Copied here rather than configured, because `elk.nodeSize.minimum` is a whole-region size
 *  while cytoscape's `min-width` applies to the children alone. `regionSize.test.ts` compares this value
 *  against what the layouter actually leaves. */
export const ELK_REGION_PADDING = 12;

/** Minimum width, in pixels, for the children of an expanded region. Cytoscape's `min-width` on
 *  `:parent` compares this against the bounding box of the children, then pads the region out to it.
 *
 *  Room for the counter chip is reserved on both sides of the name, even though the chip only sits on
 *  the right: cytoscape centers the label on the region, so reserving on one side alone would still
 *  leave the name under the chip.
 *
 *  The region's own padding lies inside the band that the name is drawn in, so it already covers part of
 *  the name and is subtracted here. */
export function regionMinWidth(label: string, leafCount: number): number {
    // A region with nothing to count has no counter to clear: `chips.ts` leaves that slot empty.
    const chipRoom = leafCount === 0
        ? 0
        : CHIP_INSET + badgePillWidth(formatLeafCount(leafCount)) + NAME_CHIP_GAP;
    return Math.max(0, Math.ceil(labelWidth(label) + 2 * chipRoom - 2 * REGION_PADDING));
}

/** The same minimum in the form ELK takes: a width for the whole region, and a string, because
 *  `cytoscape-elk` passes per-node options straight to elkjs.
 *
 *  ELK is given slightly more than cytoscape will draw, because ELK's padding around the children is the
 *  wider of the two, so a region that cytoscape widens never reaches into the space ELK gave a sibling.
 *
 *  The pair reads `(height, width)`, not the `(width, height)` that ELK documents. `layered` applies a
 *  compound node's minimum along its own internal axes, which a vertical layout direction swaps, and
 *  `cytograph.ts` lays the diagram out with `elk.direction: DOWN`. A sideways direction would stretch
 *  every region to the height of its name instead, so `DOWN` is a precondition of this function.
 *  `regionSize.test.ts` checks both directions against the real layouter. */
export function elkRegionMinimumSize(childrenMinWidth: number): string {
    return `(0,${Math.ceil(childrenMinWidth + 2 * ELK_REGION_PADDING)})`;
}

/** Per-node options for the ELK layout, in the form `cytoscape-elk`'s `nodeLayoutOptions` asks for: a
 *  minimum size for an expanded region, and nothing for any other node. Only a minimum is set, so ELK
 *  still sizes a region from the nodes it holds whenever those are the wider of the two. */
export function elkNodeLayoutOptions(node: {
    isParent(): boolean;
    data(name: string): unknown;
}): Record<string, string> {
    if (!node.isParent()) {
        return {};
    }
    return {
        'elk.nodeSize.constraints': 'MINIMUM_SIZE',
        'elk.nodeSize.minimum': elkRegionMinimumSize(Number(node.data('min_width')) || 0)
    };
}
