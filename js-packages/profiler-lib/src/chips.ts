// Corner chips: two small pills stacked on the top right corner of a diagram node.
//
// Slot 0, resting on the top edge from outside, marks operators that have SQL source attached; slot 1,
// just inside the top edge, carries the number of primitive operators inside a composite. Both are
// buttons - `chipButtons.ts` owns their hit boxes, their actions, and the window control that replaces
// the count while a composite is hovered.
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
export const BADGE_PAD_X = 8;
export const CHIP_HEIGHT = 16;
/** Corner radius of the code chip; the counter stays a pill. */
export const CODE_RADIUS = 4;
/** Advance width of one monospace glyph at `CHIP_FONT_SIZE`. Whatever monospace font the browser
 *  resolves, `textLength` pins the glyph run to exactly this per character. */
const CHIP_GLYPH_WIDTH = CHIP_FONT_SIZE * 0.6;
/** Baseline offset that centers a run of digits vertically in the pill. */
const CHIP_BASELINE = CHIP_HEIGHT / 2 + CHIP_FONT_SIZE * 0.36;

/** Gap around the counter chip: how far inside the node's top right corner it sits, and so also the
 *  gap between it and the code chip above it. */
export const CHIP_INSET = 1;

/** Bounds expansion (`[top, right, bottom, left]`) covering the code chip, the one part of a node drawn
 *  outside its own box. The render cache clips to the bounding box, so an uncovered side cuts the chip
 *  off. */
export const CHIP_BOUNDS_EXPANSION: [number, number, number, number] = [CHIP_HEIGHT, 0, 0, 0];

const chipWidth = (glyphs: number, padX: number): number => glyphs * CHIP_GLYPH_WIDTH + 2 * padX;

/** `</>`, escaped for SVG text. */
const CODE_LABEL = '&lt;/&gt;';
const CODE_GLYPHS = 3;
export const CODE_CHIP_WIDTH = chipWidth(CODE_GLYPHS, CODE_PAD_X);
/** The counter canvas is sized for the widest label `formatLeafCount` can produce, and its pill drawn
 *  right-aligned inside it, so a wider count grows leftward from a fixed corner anchor. The rest of the
 *  canvas is transparent. */
export const BADGE_CANVAS_WIDTH = chipWidth('1000M'.length, BADGE_PAD_X);

/** Width of the pill drawn in the counter slot, as opposed to its canvas: what is seen, and what
 *  `chipButtons.ts` treats as the button. */
export const badgePillWidth = (label: string): number =>
    Math.min(BADGE_CANVAS_WIDTH, chipWidth(label.length, BADGE_PAD_X));

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
    'background-height': [`${CHIP_HEIGHT}px`, `${CHIP_HEIGHT}px`],
    // Both chips are anchored by their right edge and stacked on the top right corner: the code chip
    // rests on the top edge from outside, the counter sits `CHIP_INSET` px inside it - which is also
    // the gap between the two when a node carries both.
    'background-position-x': ['100%', '100%'],
    'background-position-y': ['0%', '0%'],
    'background-offset-x': [`${-CHIP_INSET}px`, `${-CHIP_INSET}px`],
    'background-offset-y': [`${-CHIP_HEIGHT}px`, `${CHIP_INSET}px`],
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
    radius: number,
    theme: DiagramTheme,
    content: (boxLeft: number) => string
): string {
    const p = DIAGRAM_PALETTES[theme];
    const boxLeft = canvasWidth - boxWidth;
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${round(canvasWidth)}" height="${CHIP_HEIGHT}"`
        + ` viewBox="0 0 ${round(canvasWidth)} ${CHIP_HEIGHT}">`
        + `<rect x="${round(boxLeft + 0.5)}" y="0.5" width="${round(boxWidth - 1)}" height="${CHIP_HEIGHT - 1}"`
        + ` rx="${round(radius)}" fill="${p.chipFill}" stroke="${p.chipBorder}" stroke-width="1"/>`
        + content(boxLeft)
        + `</svg>`;
}

/** A chip holding `label`. `glyphCount` is the label's rendered character count, which differs from
 *  its length when the label carries SVG entities. */
function textChip(
    label: string,
    glyphCount: number,
    canvasWidth: number,
    padX: number,
    radius: number,
    theme: DiagramTheme
): string {
    const textWidth = glyphCount * CHIP_GLYPH_WIDTH;
    const boxWidth = Math.min(canvasWidth, textWidth + 2 * padX);
    return chipSvg(canvasWidth, boxWidth, radius, theme, (boxLeft) =>
        `<text x="${round(boxLeft + boxWidth / 2)}" y="${round(CHIP_BASELINE)}"`
        + ` textLength="${round(textWidth)}" lengthAdjust="spacingAndGlyphs" text-anchor="middle"`
        + ` fill="${DIAGRAM_PALETTES[theme].chipInk}" font-family="monospace"`
        + ` font-size="${CHIP_FONT_SIZE}">${label}</text>`);
}

/** Side of the square and length of the dash in the window controls, and the stroke both are drawn
 *  with. */
const CONTROL_GLYPH = 7;
const CONTROL_STROKE = 1.5;

/** A chip holding a window control instead of a label: a square for expanding a composite, a dash for
 *  collapsing it. */
function controlChip(
    control: 'expand' | 'collapse',
    canvasWidth: number,
    boxWidth: number,
    theme: DiagramTheme
): string {
    const ink = DIAGRAM_PALETTES[theme].chipInk;
    return chipSvg(canvasWidth, boxWidth, CHIP_HEIGHT / 2, theme, (boxLeft) => {
        const x = round(boxLeft + (boxWidth - CONTROL_GLYPH) / 2);
        return control === 'expand'
            ? `<rect x="${x}" y="${round((CHIP_HEIGHT - CONTROL_GLYPH) / 2)}"`
            + ` width="${CONTROL_GLYPH}" height="${CONTROL_GLYPH}" fill="none"`
            + ` stroke="${ink}" stroke-width="${CONTROL_STROKE}"/>`
            : `<rect x="${x}" y="${round((CHIP_HEIGHT - CONTROL_STROKE) / 2)}"`
            + ` width="${CONTROL_GLYPH}" height="${CONTROL_STROKE}" fill="${ink}"/>`;
    });
}

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
        hasSource
            ? cached(`code:${theme}`,
                () => textChip(CODE_LABEL, CODE_GLYPHS, CODE_CHIP_WIDTH, CODE_PAD_X, CODE_RADIUS, theme))
            : CHIP_NONE,
        leafCount === 0
            ? CHIP_NONE
            : counter === 'count'
                ? cached(`count:${theme}:${count}`,
                    () => textChip(count, count.length, BADGE_CANVAS_WIDTH, BADGE_PAD_X, CHIP_HEIGHT / 2, theme))
                // The control keeps the width of the count it stands in for, so the button does not
                // resize under the pointer that is already on it.
                : cached(`${counter}:${theme}:${badgePillWidth(count)}`,
                    () => controlChip(counter, BADGE_CANVAS_WIDTH, badgePillWidth(count), theme)),
    ];
}
