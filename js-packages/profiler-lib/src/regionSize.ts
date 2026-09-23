// How wide an expanded region has to be for its own name.
//
// A cytoscape parent is sized by the nodes it holds and ignores its own label, so a region holding one
// narrow operator would be narrower than the name drawn in the band along its top edge, leaving the name
// running past both borders and under the counter chip in the corner.
//
// The width that name needs has to reach both systems with a say in the geometry: cytoscape draws the
// region box, while ELK spaces the region's siblings by what it computes the region takes. Telling only
// cytoscape widens a region into its neighbour; telling only ELK opens a gap no region grows into. Hence
// one number here and two spellings of it.

import { badgePillWidth, CHIP_INSET, formatLeafCount } from './chips.js';
import { labelWidth, REGION_PADDING } from './diagramTheme.js';

/** Gap between the region's name and the counter chip beside it, so the two read as two things. */
const NAME_CHIP_GAP = 6;

/** ELK's own padding inside a compound node: the room it leaves between a region's border and the
 *  nodes it holds. Mirrored here, not set - `elkRegionMinimumSize` is a whole-region width, while
 *  cytoscape measures `min-width` against the children alone. */
const ELK_REGION_PADDING = 12;

/** Minimum width for the children of an expanded region: what cytoscape's `min-width` on `:parent`
 *  takes, comparing it against their bounding box and padding the region out to it.
 *
 *  Room for the counter chip is left on both sides of the name, not only on the right where the chip
 *  sits: the name is centered on the region, so it clears the chip only if the reservation is
 *  symmetric. The region's own padding is part of the band the name is drawn in, so it counts towards
 *  the name and comes off here. */
export function regionMinWidth(label: string, leafCount: number): number {
    // No counter to clear when there is nothing to count, `chips.ts` leaving that slot empty.
    const chipRoom = leafCount === 0
        ? 0
        : CHIP_INSET + badgePillWidth(formatLeafCount(leafCount)) + NAME_CHIP_GAP;
    return Math.max(0, Math.ceil(labelWidth(label) + 2 * chipRoom - 2 * REGION_PADDING));
}

/** The same minimum as ELK takes it: a whole-region width, and a string, since `cytoscape-elk` hands
 *  per-node options straight to elkjs.
 *
 *  ELK is given a hair more than cytoscape will draw, its compound padding being the wider of the two,
 *  so a widened region never reaches into the space a sibling was placed in.
 *
 *  The pair reads `(height, width)`, not the `(width, height)` ELK documents: the minimum of a compound
 *  node is applied in `layered`'s internal axes, which a vertical layout direction has transposed.
 *  `regionSize.test.ts` pins that against the real layouter, so an elkjs that stops transposing fails
 *  there rather than by inflating every region's height. */
export function elkRegionMinimumSize(childrenMinWidth: number): string {
    return `(0,${Math.ceil(childrenMinWidth + 2 * ELK_REGION_PADDING)})`;
}

/** Per-node options for the ELK layout, as `cytoscape-elk`'s `nodeLayoutOptions` asks for them: a
 *  minimum size for an expanded region, nothing for anything else. Only a minimum is added, so ELK
 *  still sizes every region to the nodes it holds whenever those are the wider of the two. */
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
