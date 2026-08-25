// The minimum width that cytoscape and ELK both have to use. The arithmetic is checked here, and so is
// what the real ELK does with the result: the layouter is the half of the agreement that cannot be read
// off the stylesheet. That cytoscape draws a region that wide is checked in `diagramTheme.test.ts`.

import ELK from 'elkjs/lib/elk.bundled.js'
import { describe, expect, it } from 'vitest'
import { badgePillWidth, CHIP_INSET, formatLeafCount } from './chips.js'
import { labelWidth, REGION_PADDING } from './diagramTheme.js'
import {
    ELK_REGION_PADDING,
    elkNodeLayoutOptions,
    elkRegionMinimumSize,
    regionMinWidth
} from './regionSize.js'

/** Width of the counter chip drawn for `leafCount`, in the band that also holds the region's name. */
const chipWidth = (leafCount: number) => badgePillWidth(formatLeafCount(leafCount))

/** The gap `regionSize.ts` leaves between the name and the counter chip. Written out again here rather
 *  than imported, so that narrowing the gap in the source fails this test. */
const NAME_CHIP_GAP = 6

describe('regionMinWidth', () => {
    it('holds the region name clear of a counter chip on either side', () => {
        // Room on both sides, although the chip only sits on the right: the name is centered on the
        // region, so reserving on one side alone would leave it under the chip.
        const label = 'region shard_by_index'
        const min = regionMinWidth(label, 12)
        // `min-width` is measured against the children, so the region adds its padding on top of it.
        expect(min + 2 * REGION_PADDING)
            .toBeGreaterThanOrEqual(labelWidth(label) + 2 * (CHIP_INSET + chipWidth(12) + NAME_CHIP_GAP))
    })

    it('grows with the name and with the text in the counter', () => {
        expect(regionMinWidth('region longer_name', 12)).toBeGreaterThan(regionMinWidth('region n', 12))
        // The floor follows the text the counter draws, not the count behind it: `formatLeafCount`
        // shortens 1000 to the two glyphs of `1K`, which is a narrower pill than the three of `999`.
        expect(regionMinWidth('region shard', 1000)).toBeGreaterThan(regionMinWidth('region shard', 1))
        expect(regionMinWidth('region shard', 999)).toBeGreaterThan(regionMinWidth('region shard', 1000))
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
        // reserves short of the drawn box is a sibling that the region overlaps.
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

    it('falls back to no floor for a node that carries no min_width', () => {
        // `Number(undefined)` is `NaN`, which would reach ELK as the unusable minimum `(0,NaN)`.
        expect(elkNodeLayoutOptions(node(true, undefined))['elk.nodeSize.minimum'])
            .toBe(elkRegionMinimumSize(0))
    })
})

describe('what the real ELK makes of the minimum', () => {
    const elk = new ELK()
    /** Width of the one operator the test region holds. */
    const CHILD_WIDTH = 40
    /** The test region, laid out the way `cytograph.ts` lays the diagram out. */
    const laidOut = async (options: Record<string, string>, direction = 'DOWN') => {
        const result = await elk.layout({
            id: 'root',
            layoutOptions: {
                algorithm: 'layered',
                'elk.direction': direction,
                'elk.hierarchyHandling': 'INCLUDE_CHILDREN'
            },
            children: [
                {
                    id: 'region',
                    layoutOptions: options,
                    children: [{ id: 'inside', width: CHILD_WIDTH, height: 8 }]
                }
            ],
            edges: []
        })
        const region = result.children![0]!
        return { width: region.width!, height: region.height!, inside: region.children![0]! }
    }

    it('pads a region by the amount ELK_REGION_PADDING repeats', async () => {
        // `elkRegionMinimumSize` adds that padding to a width measured against the children alone. If
        // ELK's own padding were the larger of the two, the minimum would come out short of the box
        // cytoscape draws, and the layout would place a sibling inside it.
        const natural = await laidOut({})
        expect(natural.width).toBe(CHILD_WIDTH + 2 * ELK_REGION_PADDING)
    })

    it('reserves the width, and only the width', async () => {
        const natural = await laidOut({})
        const floored = await laidOut(elkNodeLayoutOptions({ isParent: () => true, data: () => 300 }))
        expect(natural.width).toBeLessThan(300)
        expect(floored.width).toBeGreaterThanOrEqual(300 + 2 * REGION_PADDING)
        expect(floored.height).toBe(natural.height)
        // The extra width goes on the right, the side cytoscape's bias puts it on as well: the node
        // inside keeps its position, so both boxes keep the same left edge.
        expect(floored.inside.x).toBe(natural.inside.x)
    })

    it('applies the minimum to the height instead when the layout runs sideways', async () => {
        // Why `elkRegionMinimumSize` writes its pair as `(height, width)`: `layered` reads a compound
        // node's minimum along its internal axes, and a vertical direction swaps the two. The diagram
        // never lays out sideways, `cytograph.ts` setting `elk.direction: DOWN` for both of its
        // layouts, but an elkjs that stopped swapping would break the spelling, and this test with it.
        const sideways = await laidOut(
            elkNodeLayoutOptions({ isParent: () => true, data: () => 300 }),
            'RIGHT'
        )
        expect(sideways.width).toBe(CHILD_WIDTH + 2 * ELK_REGION_PADDING)
        expect(sideways.height).toBeGreaterThanOrEqual(300)
    })

    it('leaves a region wider than its floor sized by the nodes inside it', async () => {
        const natural = await laidOut({})
        const floored = await laidOut(elkNodeLayoutOptions({ isParent: () => true, data: () => 5 }))
        expect(floored.width).toBe(natural.width)
    })
})
