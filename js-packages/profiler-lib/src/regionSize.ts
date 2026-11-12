// How wide an expanded region has to be to fit its own name, and how much room it leaves above the
// nodes inside it for its chip row.
//
// Cytoscape sizes a parent node from the nodes it contains and ignores the parent's own label. A region
// that holds a single narrow operator therefore comes out narrower than the name drawn in the band along
// its top edge, so the name overflows both borders and runs under the counter chip in the corner.
//
// Two systems have to use the same width. Cytoscape draws the region box, and ELK spaces the region's
// siblings by the size it computes for the region. Widening the region in cytoscape alone makes it
// overlap its neighbour; widening it in ELK alone leaves a gap that the region never fills. So the width
// is computed once here, and returned in the two forms the two systems accept.

import type { Core } from 'cytoscape';
import { BADGE_HEIGHT, badgePillWidth, CHIP_INSET, formatLeafCount } from './chips.js';
import { labelWidth, REGION_PADDING } from './diagramTheme.js';

/** Horizontal space, in pixels, left between the region's name and the counter chip next to it, so that
 *  the name does not run up against the chip. */
const NAME_CHIP_GAP = 6;

/** ELK's default `elk.padding`, in pixels: the space it leaves between a region's border and the nodes
 *  inside it. `elkRegionMinimumSize` needs it, because `elk.nodeSize.minimum` is a whole-region size
 *  while cytoscape's `min-width` applies to the children alone. `elkNodeLayoutOptions` keeps it on the
 *  left, right and bottom sides. `regionSize.test.ts` compares this value against what the layouter
 *  actually leaves. */
export const ELK_REGION_PADDING = 12;

/** The extra height, in pixels, that an expanded region adds above the nodes inside it, on top of its
 *  padding. The region draws its counter chip and its name in a row along its top edge, the chip row.
 *  The chip row starts `CHIP_INSET` px below the top edge and is `BADGE_HEIGHT` px tall. The bounding
 *  box of a node inside starts at the top of its code chip, so this extra height keeps the code chip
 *  `CHIP_INSET` px below the chip row. */
export const REGION_CHIP_ROW_ROOM = CHIP_INSET + BADGE_HEIGHT + CHIP_INSET - REGION_PADDING;

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
 *  minimum size and the padding for an expanded region, and nothing for any other node. Only a minimum
 *  is set, so ELK still sizes a region from the nodes it holds whenever those are the wider of the two.
 *  The top padding also includes `REGION_CHIP_ROW_ROOM`, the extra height that cytoscape adds above
 *  the nodes. */
export function elkNodeLayoutOptions(node: {
    isParent(): boolean;
    data(name: string): unknown;
}): Record<string, string> {
    if (!node.isParent()) {
        return {};
    }
    const side = ELK_REGION_PADDING;
    return {
        'elk.nodeSize.constraints': 'MINIMUM_SIZE',
        'elk.nodeSize.minimum': elkRegionMinimumSize(Number(node.data('min_width')) || 0),
        'elk.padding': `[top=${side + REGION_CHIP_ROW_ROOM},left=${side},bottom=${side},right=${side}]`
    };
}

/** Sets the `min_height` of each expanded region to the height of its children plus
 *  `REGION_CHIP_ROW_ROOM`. The `:parent` style puts all of the extra height at the top.
 *
 *  Cytoscape has one `padding` for all four sides of a parent, so `min-height` is the only way to add
 *  room on one side. Cytoscape compares `min-height` with the box around the children, which changes
 *  with each layout, so the caller must call this function after each layout. The function sets the
 *  inner regions first, because the size of an inner region is part of the box around the children of
 *  the region that holds it. */
export function reserveRegionChipRows(cy: Core): void {
    const regions = cy.nodes(':parent').sort((a, b) => b.ancestors().length - a.ancestors().length);
    regions.forEach((region) => {
        // Cytoscape uses these bounding box options to size a parent from its children.
        const children = region.children().boundingBox({ includeLabels: true, includeOverlays: false });
        const minHeight = children.h + REGION_CHIP_ROW_ROOM;
        // A data change restyles the region and all nodes in it, so the function skips an equal value.
        if (region.data('min_height') !== minHeight) {
            region.data('min_height', minHeight);
        }
    });
}
