import { describe, expect, it } from 'vitest'
import {
    badgePillWidth,
    BADGE_CANVAS_WIDTH,
    BADGE_PAD_X,
    CHIP_FONT_SIZE,
    CHIP_HEIGHT,
    CHIP_NONE,
    CODE_PAD_X,
    CODE_RADIUS,
    formatLeafCount,
    nodeChips
} from './chips.js'

const decode = (uri: string): string => decodeURIComponent(uri.replace('data:image/svg+xml;utf8,', ''))

// The attribute name is matched with its leading whitespace, so `width` does not also match
// `stroke-width`.
const attr = (svg: string, tag: string, name: string): number =>
    Number(new RegExp(`<${tag}[^>]*?\\s${name}="([\\d.]+)"`).exec(svg)![1])

/** Every rect in the image, in document order: the pill first, then whatever is drawn inside it. */
const rects = (svg: string) =>
    [...svg.matchAll(/<rect[^>]*>/g)].map((match) => ({
        x: attr(match[0], 'rect', 'x'),
        y: attr(match[0], 'rect', 'y'),
        width: attr(match[0], 'rect', 'width'),
        height: attr(match[0], 'rect', 'height')
    }))

const counter = (leafCount: number, glyph?: 'expand' | 'collapse') =>
    decode(nodeChips(false, leafCount, 'light', glyph)[1]!)

describe('nodeChips', () => {
    it('leaves both slots empty for a primitive operator without source', () => {
        expect(nodeChips(false, 0, 'light')).toEqual([CHIP_NONE, CHIP_NONE])
    })

    it('fills slot 0 for source, slot 1 for a region — the slot order the stylesheet assumes', () => {
        const [code, badge] = nodeChips(true, 0, 'light')
        expect(code).toMatch(/^data:image\/svg\+xml/)
        expect(badge).toBe(CHIP_NONE)

        const [noCode, count] = nodeChips(false, 12, 'light')
        expect(noCode).toBe(CHIP_NONE)
        expect(decode(count!)).toContain('>12<')
    })

    it('returns the identical string for repeated calls, so cytoscape decodes each image once', () => {
        const first = nodeChips(true, 7, 'dark')
        const second = nodeChips(true, 7, 'dark')
        expect(first[0]).toBe(second[0])
        expect(first[1]).toBe(second[1])
    })

    it('bakes the palette into the image, so a theme switch yields different chips', () => {
        const light = nodeChips(true, 7, 'light')
        const dark = nodeChips(true, 7, 'dark')
        expect(light[0]).not.toBe(dark[0])
        expect(light[1]).not.toBe(dark[1])
    })

    it('keeps the badge pill inside its canvas for the widest label', () => {
        // The badge canvas is a fixed size in the stylesheet; a pill wider than the canvas would
        // be clipped, and a taller one would distort.
        const svg = decode(nodeChips(false, 999_999_999, 'light')[1]!)
        expect(attr(svg, 'rect', 'width')).toBeLessThanOrEqual(attr(svg, 'svg', 'width'))
    })

    it('draws both chips at the node font size and the same height', () => {
        const [code, badge] = nodeChips(true, 12, 'light').map((uri) => decode(uri))
        for (const svg of [code!, badge!]) {
            expect(attr(svg, 'text', 'font-size')).toBe(CHIP_FONT_SIZE)
            expect(attr(svg, 'svg', 'height')).toBe(CHIP_HEIGHT)
        }
    })

    it('pads each chip by its own padding, the badge more widely than the code chip', () => {
        // The two paddings differ by design and are tuned in `chips.ts`; what is pinned here is that
        // each chip is built from its own constant, the badge being the roomier of the two.
        expect(BADGE_PAD_X).toBeGreaterThan(CODE_PAD_X)
        const [code, badge] = nodeChips(true, 12, 'light').map((uri) => decode(uri))
        for (const [svg, padX] of [[code!, CODE_PAD_X], [badge!, BADGE_PAD_X]] as const) {
            // Box width is the pinned glyph run plus one padding on each side.
            const box = attr(svg, 'rect', 'width') + 1 // the rect is inset by half its stroke
            expect(box - attr(svg, 'text', 'textLength')).toBeCloseTo(2 * padX, 5)
        }
    })

    it('rounds the code chip to its own radius and keeps the badge a pill', () => {
        const [code, badge] = nodeChips(true, 12, 'light').map((uri) => decode(uri))
        expect(attr(code!, 'rect', 'rx')).toBe(CODE_RADIUS)
        // The code chip is a rounded box, not a pill: anything at half the height or more would
        // make the two chips the same shape.
        expect(CODE_RADIUS).toBeLessThan(CHIP_HEIGHT / 2)
        // The badge is a pill: rounded by half its height or more.
        expect(attr(badge!, 'rect', 'rx')).toBeGreaterThanOrEqual(CHIP_HEIGHT / 2 - 0.5)
    })

    it('right-aligns the pill on its canvas, the corner both chips are anchored by', () => {
        // The counter canvas is a fixed size in the stylesheet while its pill is only as wide as the
        // count: anchored by the right edge, a wider count grows leftwards from a fixed corner
        // instead of pushing the pill off it.
        for (const leafCount of [7, 999, 999_999_999]) {
            const svg = counter(leafCount)
            const pill = rects(svg)[0]!
            // The rect is inset by half its stroke, so its right edge lands half a pixel inside.
            expect(pill.x + pill.width, String(leafCount)).toBeCloseTo(attr(svg, 'svg', 'width') - 0.5, 5)
            expect(pill.width + 1).toBeCloseTo(badgePillWidth(formatLeafCount(leafCount)), 5)
        }
    })
})

describe('counter controls', () => {
    it('replaces the count with a square to expand and a dash to collapse', () => {
        // Classic window management: a square maximizes, a dash restores.
        expect(counter(7)).toContain('<text')
        const square = rects(counter(7, 'expand'))[1]!
        const dash = rects(counter(7, 'collapse'))[1]!
        expect(square.width).toBe(square.height)
        expect(dash.width).toBeGreaterThan(dash.height)
        // The square is an outline and the dash a solid bar; neither is a glyph of a font.
        expect(counter(7, 'expand')).toContain('fill="none"')
        expect(counter(7, 'expand')).not.toContain('<text')
        expect(counter(7, 'collapse')).not.toContain('<text')
    })

    it('centers the control in the pill', () => {
        for (const glyph of ['expand', 'collapse'] as const) {
            const [pill, control] = rects(counter(7, glyph))
            expect(control!.x + control!.width / 2, glyph).toBeCloseTo(pill!.x + pill!.width / 2, 1)
            expect(control!.y + control!.height / 2, glyph).toBeCloseTo(CHIP_HEIGHT / 2, 1)
        }
    })

    it('keeps the width of the count it stands in for, so the button never resizes under the pointer', () => {
        for (const leafCount of [7, 999, 999_999_999]) {
            const width = (svg: string) => rects(svg)[0]!.width
            expect(width(counter(leafCount, 'expand')), String(leafCount))
                .toBe(width(counter(leafCount)))
            expect(width(counter(leafCount, 'collapse')), String(leafCount))
                .toBe(width(counter(leafCount)))
        }
        // Which is only worth anything if the count's own width varies in the first place.
        expect(badgePillWidth('7')).toBeLessThan(badgePillWidth('1000M'))
        expect(badgePillWidth('1000M')).toBe(BADGE_CANVAS_WIDTH)
    })

    it('leaves an empty counter slot empty, control or not', () => {
        // A primitive operator counts nothing, so it has no control to press either.
        expect(nodeChips(true, 0, 'light', 'expand')[1]).toBe(CHIP_NONE)
    })

    it('caches each control per palette and width, as it does the counts', () => {
        expect(nodeChips(false, 7, 'dark', 'expand')[1]).toBe(nodeChips(false, 7, 'dark', 'expand')[1])
        expect(nodeChips(false, 7, 'dark', 'expand')[1]).not.toBe(nodeChips(false, 7, 'light', 'expand')[1])
        expect(nodeChips(false, 7, 'dark', 'expand')[1]).not.toBe(nodeChips(false, 7, 'dark', 'collapse')[1])
    })
})

describe('formatLeafCount', () => {
    it('shows exact counts below a thousand', () => {
        expect(formatLeafCount(0)).toBe('0')
        expect(formatLeafCount(7)).toBe('7')
        expect(formatLeafCount(999)).toBe('999')
    })

    it('abbreviates larger counts so the badge stays narrow', () => {
        expect(formatLeafCount(1_000)).toBe('1K')
        expect(formatLeafCount(1_540)).toBe('1.5K')
        expect(formatLeafCount(12_345)).toBe('12K')
        expect(formatLeafCount(2_400_000)).toBe('2.4M')
    })
})
