// Corner chips: two small pills stacked on the top right corner of a diagram node.
//
// Slot 0, resting on the top edge from outside, marks operators that have SQL source attached; slot 1,
// just inside the top edge, carries a network icon and the number of primitive operators inside a
// composite. Both are buttons - `chipButtons.ts` owns their hit boxes, their actions, and the window
// control that replaces the count while a composite is hovered.
//
// Chips are cytoscape background images, not DOM overlays and not extra graph elements: the geometry
// below is the same for every node, so only the image list varies per node, and the code chip hangs
// outside the node box, leaving the node's dimensions - and so the ELK layout - untouched. Each distinct
// image is generated once and shared, so cytoscape decodes it once too.

import { DIAGRAM_PALETTES, type DiagramTheme } from './diagramTheme.js';

/** Marks a chip slot as empty; cytoscape skips the slot without loading anything. */
export const CHIP_NONE = 'none';

/** Same size as the node label. */
export const CHIP_FONT_SIZE = 12;
export const CODE_PAD_X = 4;
export const BADGE_PAD_X = 10;
export const CODE_CHIP_HEIGHT = 16;
/** The counter is the taller chip of the two: it carries an icon beside the count. */
export const BADGE_HEIGHT = 20;
/** Corner radius of the code chip; the counter stays a pill. */
export const CODE_RADIUS = 4;
/** Advance width of one monospace glyph at `CHIP_FONT_SIZE`. Whatever monospace font the browser
 *  resolves, `textLength` pins the glyph run to exactly this per character. */
const CHIP_GLYPH_WIDTH = CHIP_FONT_SIZE * 0.6;
/** Baseline that centers a run of digits vertically in a chip `height` tall. */
const baseline = (height: number): number => height / 2 + CHIP_FONT_SIZE * 0.36;

/** Side of the network icon in the counter, and the gap between it and the count. */
export const BADGE_ICON_SIZE = 12;
const BADGE_ICON_GAP = 4;

/** Gap around the counter chip: how far inside the node's top right corner it sits, and so also the
 *  gap between it and the code chip above it. */
export const CHIP_INSET = 1;

/** Bounds expansion (`[top, right, bottom, left]`) covering the code chip, the one part of a node drawn
 *  outside its own box. The render cache clips to the bounding box, so an uncovered side cuts the chip
 *  off. */
export const CHIP_BOUNDS_EXPANSION: [number, number, number, number] = [CODE_CHIP_HEIGHT, 0, 0, 0];

const chipWidth = (glyphs: number, padX: number): number => glyphs * CHIP_GLYPH_WIDTH + 2 * padX;

/** `</>`, escaped for SVG text. */
const CODE_LABEL = '&lt;/&gt;';
const CODE_GLYPHS = 3;
export const CODE_CHIP_WIDTH = chipWidth(CODE_GLYPHS, CODE_PAD_X);

/** Width of a counter pill holding `glyphs` digits: the icon and the gap after it on top of the digits
 *  and the padding around them. */
const badgeWidth = (glyphs: number): number =>
    BADGE_ICON_SIZE + BADGE_ICON_GAP + chipWidth(glyphs, BADGE_PAD_X);

/** The counter canvas is sized for the widest label `formatLeafCount` can produce, and its pill drawn
 *  right-aligned inside it, so a wider count grows leftward from a fixed corner anchor. The rest of the
 *  canvas is transparent. */
export const BADGE_CANVAS_WIDTH = badgeWidth('1000M'.length);

/** Width of the pill drawn in the counter slot, as opposed to its canvas: what is seen, and what
 *  `chipButtons.ts` treats as the button. */
export const badgePillWidth = (label: string): number =>
    Math.min(BADGE_CANVAS_WIDTH, badgeWidth(label.length));

/** Constant part of the chip styling, shared by every node. Index 0 is the code chip, index 1
 *  the counter; every array below is aligned to those two slots. */
export const CHIP_BACKGROUND_STYLE = {
    'background-image': 'data(chips)',
    // Draw over the node border, unclipped by the node shape.
    'background-image-containment': ['over', 'over'],
    'background-clip': ['none', 'none'],
    'background-repeat': ['no-repeat', 'no-repeat'],
    // 1:1 with the SVG canvas, so chips stay crisp and undistorted.
    'background-width': [`${round(CODE_CHIP_WIDTH)}px`, `${round(BADGE_CANVAS_WIDTH)}px`],
    'background-height': [`${CODE_CHIP_HEIGHT}px`, `${BADGE_HEIGHT}px`],
    // Both chips are anchored by their right edge and stacked on the top right corner: the code chip
    // rests on the top edge from outside, the counter sits `CHIP_INSET` px inside it - which is also
    // the gap between the two when a node carries both.
    'background-position-x': ['100%', '100%'],
    'background-position-y': ['0%', '0%'],
    'background-offset-x': [`${-CHIP_INSET}px`, `${-CHIP_INSET}px`],
    'background-offset-y': [`${-CODE_CHIP_HEIGHT}px`, `${CHIP_INSET}px`],
} as const;

const cache = new Map<string, string>();

function round(x: number): number {
    return Math.round(x * 100) / 100;
}

const svgUri = (svg: string): string => `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;

function cached(key: string, build: () => string): string {
    let uri = cache.get(key);
    if (uri === undefined) {
        uri = svgUri(build());
        cache.set(key, uri);
    }
    return uri;
}

/** A chip: a rounded box `boxWidth` wide, right-aligned on a `canvasWidth` wide transparent canvas,
 *  holding whatever `content` draws at the box's own left edge. */
function chipSvg(
    canvasWidth: number,
    boxWidth: number,
    height: number,
    radius: number,
    theme: DiagramTheme,
    content: (boxLeft: number) => string
): string {
    const p = DIAGRAM_PALETTES[theme];
    const boxLeft = canvasWidth - boxWidth;
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${round(canvasWidth)}" height="${height}"`
        + ` viewBox="0 0 ${round(canvasWidth)} ${height}">`
        + `<rect x="${round(boxLeft + 0.5)}" y="0.5" width="${round(boxWidth - 1)}" height="${height - 1}"`
        + ` rx="${round(radius)}" fill="${p.chipFill}" stroke="${p.chipBorder}" stroke-width="1"/>`
        + content(boxLeft)
        + `</svg>`;
}

/** A run of glyphs starting at `x`, its advance pinned to `width` whatever monospace font the browser
 *  resolves. */
const glyphRun = (
    label: string,
    x: number,
    width: number,
    height: number,
    theme: DiagramTheme
): string =>
    `<text x="${round(x)}" y="${round(baseline(height))}" textLength="${round(width)}"`
    + ` lengthAdjust="spacingAndGlyphs" fill="${DIAGRAM_PALETTES[theme].chipInk}"`
    + ` font-family="monospace" font-size="${CHIP_FONT_SIZE}">${label}</text>`;

/** The `fd-network` icon web-console, drawn as strokes on a 24px grid. */
const NETWORK_ICON = '<rect x="9" y="2" width="6" height="6" rx="2"/>'
    + '<rect x="2" y="16" width="6" height="6" rx="2"/>'
    + '<rect x="16" y="16" width="6" height="6" rx="2"/>'
    + '<path d="M5 16v-3a1 1 0 0 1 1-1h12a1 1 0 0 1 1 1v3"/>'
    + '<path d="M12 12V8"/>';
const ICON_GRID = 24;

/** The icon, its top left corner at (`x`, `y`). The stroke is scaled along with the shapes, so a 12px
 *  icon is drawn with a one pixel line. */
const networkIcon = (x: number, y: number, ink: string): string =>
    `<g transform="translate(${round(x)} ${round(y)}) scale(${round(BADGE_ICON_SIZE / ICON_GRID)})"`
    + ` fill="none" stroke="${ink}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">`
    + NETWORK_ICON
    + `</g>`;

/** Side of the square and length of the dash in the window controls, and the stroke both are drawn
 *  with. */
const CONTROL_GLYPH = 7;
const CONTROL_STROKE = 1.5;

/** A window control, centered on a box `width` wide starting at `left`: a square for expanding a
 *  composite, a dash for collapsing it. */
const controlGlyph = (control: 'expand' | 'collapse', left: number, width: number, ink: string): string => {
    const x = round(left + (width - CONTROL_GLYPH) / 2);
    return control === 'expand'
        ? `<rect x="${x}" y="${round((BADGE_HEIGHT - CONTROL_GLYPH) / 2)}"`
        + ` width="${CONTROL_GLYPH}" height="${CONTROL_GLYPH}" fill="none"`
        + ` stroke="${ink}" stroke-width="${CONTROL_STROKE}"/>`
        : `<rect x="${x}" y="${round((BADGE_HEIGHT - CONTROL_STROKE) / 2)}"`
        + ` width="${CONTROL_GLYPH}" height="${CONTROL_STROKE}" fill="${ink}"/>`;
};

/** Compact label for a leaf count, so the counter stays inside `BADGE_CANVAS_WIDTH`. */
export function formatLeafCount(count: number): string {
    if (count < 1000) {
        return String(count);
    }
    const [divisor, suffix] = count < 1_000_000 ? [1000, 'K'] : [1_000_000, 'M'];
    const scaled = count / divisor!;
    return `${scaled < 10 ? Math.round(scaled * 10) / 10 : Math.round(scaled)}${suffix}`;
}

/** What the counter slot shows: how many primitive operators a composite holds, or - while that
 *  composite is hovered - the control that expands or collapses it. */
export type CounterGlyph = 'count' | 'expand' | 'collapse';

/** The counter chip: the icon and `label`, or the control that stands in for both while the composite
 *  is hovered. `label` sets the width either way, so the button does not resize under the pointer that
 *  is already on it. */
function counterChip(label: string, glyph: CounterGlyph, theme: DiagramTheme): string {
    const boxWidth = badgePillWidth(label);
    return chipSvg(BADGE_CANVAS_WIDTH, boxWidth, BADGE_HEIGHT, BADGE_HEIGHT / 2, theme, (boxLeft) => {
        const ink = DIAGRAM_PALETTES[theme].chipInk;
        if (glyph !== 'count') {
            return controlGlyph(glyph, boxLeft, boxWidth, ink);
        }
        return networkIcon(boxLeft + BADGE_PAD_X, (BADGE_HEIGHT - BADGE_ICON_SIZE) / 2, ink)
            + glyphRun(label, boxLeft + BADGE_PAD_X + BADGE_ICON_SIZE + BADGE_ICON_GAP,
                label.length * CHIP_GLYPH_WIDTH, BADGE_HEIGHT, theme);
    });
}

/** The code chip: `</>` on a rounded box of its own. */
const codeChip = (theme: DiagramTheme): string =>
    chipSvg(CODE_CHIP_WIDTH, CODE_CHIP_WIDTH, CODE_CHIP_HEIGHT, CODE_RADIUS, theme, (boxLeft) =>
        glyphRun(CODE_LABEL, boxLeft + CODE_PAD_X, CODE_GLYPHS * CHIP_GLYPH_WIDTH,
            CODE_CHIP_HEIGHT, theme));

/** The two chip slots for a node: `[code chip, counter]`, each either an image URI or `CHIP_NONE`.
 *  Assigned to the node's `chips` data field, which the stylesheet maps to `background-image`. */
export function nodeChips(
    hasSource: boolean,
    leafCount: number,
    theme: DiagramTheme,
    counter: CounterGlyph = 'count'
): Array<string> {
    const count = formatLeafCount(leafCount);
    return [
        hasSource ? cached(`code:${theme}`, () => codeChip(theme)) : CHIP_NONE,
        leafCount === 0
            ? CHIP_NONE
            : counter === 'count'
                ? cached(`count:${theme}:${count}`, () => counterChip(count, 'count', theme))
                : cached(`${counter}:${theme}:${count.length}`,
                    () => counterChip(count, counter, theme)),
    ];
}
