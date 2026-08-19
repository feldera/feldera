// Node text: the two runs a node's text is made of - its id, then the operator name - painted on the
// canvas. Every node carries it, an expanded region included: a region shows its name in the band
// along its top edge, where it names what it contains without covering any of it.
//
// A cytoscape label is one run of one style, and the id has to stand out from the operator name it
// precedes. So the label is kept for measurement only (`text-opacity: 0`, measured at the id's heavier
// weight so the node is never sized smaller than what is drawn) and the runs are drawn here, through the
// same kind of renderer tap `nodeShadow.ts` uses: `drawNodeOverlay`, which runs after the node body with
// the context in graph coordinates.

import type { Core, NodeSingular } from 'cytoscape';
import { CHIP_HEIGHT, CHIP_INSET } from './chips.js';
import {
    DIAGRAM_PALETTES,
    type DiagramTheme,
    HEAT_TEXT_FLIP,
    ID_FONT_WEIGHT,
    NODE_OUTER_HEIGHT
} from './diagramTheme.js';

/** One styled run of a node's text. */
export interface TextRun {
    text: string;
    /** CSS font weight, as it goes into the canvas font shorthand. Weight is what separates the id
     *  from the operator name at a glance. */
    weight: string;
    color: string
}

/** The runs of a node's text, in drawing order. `heat` is the node's metric value on the 0-100 scale the
 *  fill is mapped from, and `isRegion` says whether the node is an expanded region, which is drawn in the
 *  region tint whatever its metric. */
export function nodeTextRuns(
    id: string,
    operator: string,
    heat: number,
    theme: DiagramTheme,
    isRegion: boolean
): TextRun[] {
    const p = DIAGRAM_PALETTES[theme];
    // Only text drawn on the heat fill flips. A region carries the metric of everything inside it but
    // keeps the region tint, so one hot descendant must not turn its name white.
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

/** Where the text of a node is centered, in graph coordinates.
 *
 *  An expanded region carries its name in the band along its top edge, on the line of its counter chip:
 *  the interior belongs to the nodes it contains. Every other node keeps its text in the bottom
 *  `NODE_OUTER_HEIGHT` of itself, which is all of an operator and the second row of a collapsed
 *  composite. */
export function textCenter(
    position: { x: number, y: number },
    outerHeight: number,
    isRegion: boolean
): { x: number, y: number } {
    if (isRegion) {
        return { x: position.x, y: position.y - outerHeight / 2 + CHIP_INSET + CHIP_HEIGHT / 2 };
    }
    return { x: position.x, y: position.y + (outerHeight - NODE_OUTER_HEIGHT) / 2 };
}

/** The slice of the canvas API the painter needs. Narrower than `CanvasRenderingContext2D` so the
 *  measuring can be exercised without one. */
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

/** Paint `runs` as one line, centered on (`centerX`, `centerY`), separated by the width of a space -
 *  the same separator the measured label carries between id and operator name. */
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

/** Renderer entry point wrapped by `installNodeText`: `(context, node, pos?, w?, h?)`, called once
 *  per node per frame, after the node's body and its chips. */
type DrawNodeOverlay = (
    context: CanvasRenderingContext2D,
    node: NodeSingular,
    pos?: { x: number, y: number },
    w?: number,
    h?: number
) => void;

/** Start painting node text on `cy`. Call once per instance, after construction. `theme` is read on
 *  every frame, so switching the palette needs no more than the repaint it already does. */
export function installNodeText(cy: Core, theme: () => DiagramTheme): void {
    // As in `nodeShadow.ts`: not a documented API, so only the first two arguments are read and the
    // rest are passed straight through.
    const renderer = (cy as unknown as { renderer(): Record<string, unknown> }).renderer();
    const original = renderer['drawNodeOverlay'] as DrawNodeOverlay | undefined;
    if (typeof original !== 'function') {
        // A headless instance has no canvas renderer, and so nothing to draw text on.
        return;
    }
    renderer['drawNodeOverlay'] = function (
        this: unknown,
        context: CanvasRenderingContext2D,
        node: NodeSingular,
        ...rest: [({ x: number, y: number } | undefined)?, (number | undefined)?, (number | undefined)?]
    ): void {
        if (node.visible()) {
            const isRegion = node.isParent();
            const center = textCenter(rest[0] ?? node.position(), node.outerHeight(), isRegion);
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
        }
        original.call(this, context, node, ...rest);
    } as DrawNodeOverlay;
}
