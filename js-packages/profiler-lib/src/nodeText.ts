// Draws the text of each circuit diagram node on the canvas. The text has two parts, called runs,
// and each run has its own style, so that the id is easy to see next to the operator name:
//
//   Run             Font weight   Color
//   node id         semibold      normal text color
//   operator name   normal        muted text color
//
// A cytoscape label has only one style, so the cytoscape label of each node is invisible
// (`text-opacity: 0`). This file paints the runs with `fillText` on the cytoscape canvas, in the same
// frame as the node, in the `drawNodeOverlay` layer of `cytoscapeRenderer.ts`, above the node body and
// its chips.
//
// The node width is calculated from the full text in the semibold font weight (`labelWidth` in
// `diagramTheme.ts`). Semibold text is wider than normal text, so the drawn text is never wider than
// the calculated width.

import type { Core } from 'cytoscape';
import { BADGE_HEIGHT, CHIP_INSET } from './chips.js';
import { paintNodeLayer } from './cytoscapeRenderer.js';
import {
    DIAGRAM_PALETTES,
    type DiagramTheme,
    HEAT_TEXT_FLIP,
    ID_FONT_WEIGHT,
    NODE_OUTER_HEIGHT
} from './diagramTheme.js';

/** One part of a node's text, drawn in one font weight and one color. */
export interface TextRun {
    text: string;
    /** CSS font weight, in the format of the canvas `font` property. */
    weight: string;
    color: string
}

/** The runs of a node's text, in the order to draw them: the id, then the operator name if there is
 *  one. `heat` is the metric value of the node, from 0 to 100. `isRegion` is true for an expanded
 *  circuit region. */
export function nodeTextRuns(
    id: string,
    operator: string,
    heat: number,
    theme: DiagramTheme,
    isRegion: boolean
): TextRun[] {
    const p = DIAGRAM_PALETTES[theme];
    // Above `HEAT_TEXT_FLIP`, the node background is a strong red, so the text is white (`textOnHeat`).
    // The background of an expanded region is always the `region` color, so its text keeps the normal
    // colors, also when `heat` is high.
    const onHeat = !isRegion && heat > HEAT_TEXT_FLIP;
    const runs: TextRun[] = [
        { text: id, weight: `${ID_FONT_WEIGHT}`, color: onHeat ? p.textOnHeat : p.text }
    ];
    if (operator !== '') {
        runs.push({
            text: operator,
            weight: 'normal',
            color: onHeat ? p.textOnHeat : p.textMuted
        });
    }
    return runs;
}

/** The center of a node's text, in graph coordinates. An expanded circuit region shows its name along
 *  its top edge, level with its counter chip, because the nodes inside the region fill the rest of it.
 *  Other nodes show their text in their bottom `NODE_OUTER_HEIGHT`. For an operator, that is the full
 *  node. For a collapsed region, that is the row below the counter chip. */
export function textCenter(
    position: { x: number, y: number },
    outerHeight: number,
    isRegion: boolean
): { x: number, y: number } {
    if (isRegion) {
        return { x: position.x, y: position.y - outerHeight / 2 + CHIP_INSET + BADGE_HEIGHT / 2 };
    }
    return { x: position.x, y: position.y + (outerHeight - NODE_OUTER_HEIGHT) / 2 };
}

/** The part of `CanvasRenderingContext2D` that `paintTextRuns` uses, so that tests can use a simple
 *  mock instead of a real canvas. */
export interface TextContext {
    font: string;
    fillStyle: string | CanvasGradient | CanvasPattern;
    textAlign: CanvasTextAlign;
    textBaseline: CanvasTextBaseline;
    globalAlpha: number;
    measureText(text: string): { width: number };
    fillText(text: string, x: number, y: number): void;
    save(): void;
    restore(): void;
}

/** Draw `runs` on one line, centered on (`centerX`, `centerY`). The gap between two runs is one space
 *  in the normal font weight. The calculated node width also includes one space between the id and the
 *  operator name. */
export function paintTextRuns(
    context: TextContext,
    runs: TextRun[],
    fontSize: number,
    fontFamily: string,
    centerX: number,
    centerY: number
): void {
    const font = (run: TextRun) => `${run.weight} ${fontSize}px ${fontFamily}`;
    context.save();
    context.textAlign = 'left';
    context.textBaseline = 'middle';
    const widths = runs.map((run) => {
        context.font = font(run);
        return context.measureText(run.text).width;
    });
    context.font = `normal ${fontSize}px ${fontFamily}`;
    const gap = context.measureText(' ').width;
    const total = widths.reduce((a, b) => a + b, 0) + gap * (runs.length - 1);

    let x = centerX - total / 2;
    runs.forEach((run, i) => {
        context.font = font(run);
        context.fillStyle = run.color;
        context.globalAlpha = 1;
        context.fillText(run.text, x, centerY);
        x += widths[i]! + gap;
    });
    context.restore();
}

/** Start to draw node text on `cy`. Call this one time for each cytoscape instance, after you create
 *  it. Each frame reads `theme`, so the first repaint after a theme change uses the new colors. */
export function installNodeText(cy: Core, theme: () => DiagramTheme): void {
    paintNodeLayer(cy, 'drawNodeOverlay', (context, node, body) => {
        const isRegion = node.isParent();
        const center = textCenter(body.center, node.outerHeight(), isRegion);
        paintTextRuns(
            context,
            nodeTextRuns(
                node.id(),
                String(node.data('operator') ?? ''),
                Number(node.data('value')) || 0,
                theme(),
                isRegion),
            Number(node.numericStyle('font-size')),
            String(node.style('font-family')),
            center.x,
            center.y
        );
    });
}
