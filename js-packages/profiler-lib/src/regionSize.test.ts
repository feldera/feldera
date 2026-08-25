// The one width two sizing systems have to agree on. The arithmetic is checked here, together with what
// the real ELK does with it - the layouter is the half of the agreement that cannot be read off the
// stylesheet. That cytoscape draws a region that wide is in `diagramTheme.test.ts`.

import ELK from 'elkjs/lib/elk.bundled.js'
import { describe, expect, it } from 'vitest'
import { badgePillWidth, formatLeafCount } from './chips.js'
import { labelWidth, REGION_PADDING } from './diagramTheme.js'
import { elkNodeLayoutOptions, elkRegionMinimumSize, regionMinWidth } from './regionSize.js'

/** Room the counter chip takes in the band the region's name is drawn in. */
const chipWidth = (leafCount: number) => badgePillWidth(formatLeafCount(leafCount))

describe('regionMinWidth', () => {
    it('holds the region name and a counter chip clear of it on either side', () => {
        // Both sides, though the chip only sits on the right: the name is centered on the region, so
        // reserving on one side alone would push it under the chip.
        const label = 'region shard_by_index'
        const min = regionMinWidth(label, 12)
        // What cytoscape draws around the children it is measured against.
        expect(min + 2 * REGION_PADDING).toBeGreaterThanOrEqual(labelWidth(label) + 2 * chipWidth(12))
    })

    it('grows with the name and with the count beside it', () => {
        expect(regionMinWidth('region longer_name', 12)).toBeGreaterThan(regionMinWidth('region n', 12))
        // A four-glyph count is a wider pill than a one-glyph count, and takes that much more room.
        expect(regionMinWidth('region shard', 1000)).toBeGreaterThan(regionMinWidth('region shard', 1))
    })

    it('reserves nothing for a counter that is not drawn', () => {
        // `chips.ts` leaves the counter slot empty at a leaf count of zero.
        expect(regionMinWidth('region shard', 0)).toBeLessThan(regionMinWidth('region shard', 1))
    })

    it('asks for no floor at all when the region padding already covers the name', () => {
        // Never negative: `min-width` is a width, and cytoscape would take a negative one literally.
        expect(regionMinWidth('n', 0)).toBe(0)
    })
})

describe('elkRegionMinimumSize', () => {
    it('gives ELK at least the width cytoscape will draw', () => {
        // ELK spaces a region's siblings by its own idea of how wide the region is, so anything it
        // reserves short of the drawn box is a sibling the region overlaps.
        const [height, width] = elkRegionMinimumSize(200)
            .match(/^\((\d+(?:\.\d+)?),(\d+(?:\.\d+)?)\)$/)!
            .slice(1)
            .map(Number) as [number, number]
        expect(height).toBe(0)
        expect(width).toBeGreaterThanOrEqual(200 + 2 * REGION_PADDING)
    })
})

describe('elkNodeLayoutOptions', () => {
    const node = (isParent: boolean, minWidth: unknown) => ({
        isParent: () => isParent,
        data: () => minWidth
    })

    it('gives an expanded region a minimum size and everything else nothing', () => {
        expect(elkNodeLayoutOptions(node(false, 300))).toEqual({})
        const options = elkNodeLayoutOptions(node(true, 300))
        expect(options['elk.nodeSize.constraints']).toBe('MINIMUM_SIZE')
        expect(options['elk.nodeSize.minimum']).toBe(elkRegionMinimumSize(300))
    })

    it('survives a node with no floor on it', () => {
        // Every node definition carries `min_width`, but a graph built before it did, or a node
        // cytoscape holds for another reason, has none.
        expect(elkNodeLayoutOptions(node(true, undefined))['elk.nodeSize.minimum'])
            .toBe(elkRegionMinimumSize(0))
    })
})

describe('what the real ELK makes of the minimum', () => {
    const elk = new ELK()
    /** A region holding one 40px operator, laid out the way the diagram lays out. */
    const laidOut = async (options: Record<string, string>) => {
        const result = await elk.layout({
            id: 'root',
            layoutOptions: {
                algorithm: 'layered',
                'elk.direction': 'DOWN',
                'elk.hierarchyHandling': 'INCLUDE_CHILDREN'
            },
            children: [
                {
                    id: 'region',
                    layoutOptions: options,
                    children: [{ id: 'inside', width: 40, height: 8 }]
                }
            ],
            edges: []
        })
        const region = result.children![0]!
        return { width: region.width!, height: region.height!, inside: region.children![0]! }
    }

    it('reserves the width, and only the width', async () => {
        // The claim `elkRegionMinimumSize` rests on. A compound node's minimum is applied in
        // `layered`'s internal axes, which a vertical direction has transposed - so the pair reads
        // `(height, width)` here. An elkjs that stops transposing fails this test instead of silently
        // stretching every region to the height of its own name.
        const natural = await laidOut({})
        const floored = await laidOut(elkNodeLayoutOptions({ isParent: () => true, data: () => 300 }))
        expect(natural.width).toBeLessThan(300)
        expect(floored.width).toBeGreaterThanOrEqual(300 + 2 * REGION_PADDING)
        expect(floored.height).toBe(natural.height)
        // The extra goes on the right, which is the side cytoscape's bias puts it on too: the node
        // inside stays where it was, so both boxes keep the same left edge.
        expect(floored.inside.x).toBe(natural.inside.x)
    })

    it('leaves a region wider than its floor sized by the nodes inside it', async () => {
        const natural = await laidOut({})
        const floored = await laidOut(elkNodeLayoutOptions({ isParent: () => true, data: () => 5 }))
        expect(floored.width).toBe(natural.width)
    })
})
