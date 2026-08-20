// Color palettes for the circuit diagram, and the cytoscape stylesheet built from them.

import type { StylesheetJson } from 'cytoscape';
import { CHIP_BACKGROUND_STYLE, CHIP_BOUNDS_EXPANSION } from './chips.js';

/** Regions are translucent, so nesting reads as deepening tint instead of a color per depth. */
export const REGION_OPACITY = 0.1;

/** Space between a region's border and the nodes it holds. The region's own name and counter chip are
 *  drawn in that band. Also used by `regionSize.ts`. */
export const REGION_PADDING = 10;

/** Horizontal padding of an operator node. Cytoscape pads all four sides, so the heights below take
 *  off the padding and border it adds. */
export const NODE_PADDING_X = 8;
const NODE_BORDER_WIDTH = 1;
export const NODE_OUTER_HEIGHT = 25;
/** The node box itself, which is what `renderedHeight` measures, so `FOCUS_ZOOM` is picked against it. */
export const NODE_INNER_HEIGHT = NODE_OUTER_HEIGHT - 2 * NODE_PADDING_X - NODE_BORDER_WIDTH;
/** First row of a collapsed composite, holding its counter chip, with the text in the second row. Has
 *  to fit the chip and the gap around it, which cannot be read from `chips.ts` here: the two modules
 *  import each other, so cross-reads only work inside a function body. `diagramTheme.test.ts` pins the
 *  relation instead. */
const BADGE_ROW_HEIGHT = 24;
export const COMPOSITE_OUTER_HEIGHT = NODE_OUTER_HEIGHT + BADGE_ROW_HEIGHT;
const COMPOSITE_INNER_HEIGHT = NODE_INNER_HEIGHT + BADGE_ROW_HEIGHT;

/** Corner radius per node kind. All three are round rectangles, so the radius is what tells them
 *  apart at a glance. */
export const NODE_CORNER_RADIUS = 3;
export const COMPOSITE_CORNER_RADIUS = 8;
export const REGION_CORNER_RADIUS = 8;

/** Weight of the id run of a node's text. The whole label is measured at this weight and the operator
 *  name drawn lighter, so the measurement is never short of what is drawn. */
export const ID_FONT_WEIGHT = 600;

/** Font the label is measured in and the node text drawn in. Spelled out rather than left to
 *  cytoscape's default, because `chips.ts` reserves room inside a node by measuring spaces in the
 *  same font. */
export const NODE_FONT_SIZE = 12;
export const NODE_FONT_FAMILY = 'Helvetica Neue, Helvetica, sans-serif';
/** The font as a canvas shorthand, in the order cytoscape builds it: style, weight, size, family. */
export const labelFont = (weight: number | string = ID_FONT_WEIGHT): string =>
    `normal ${weight} ${NODE_FONT_SIZE}px ${NODE_FONT_FAMILY}`;

/** Character width used when there is no DOM to measure in. Nothing is drawn then either, so it only
 *  has to be plausible. */
const LABEL_GLYPH_FALLBACK = NODE_FONT_SIZE * 0.55;
let labelContext: CanvasRenderingContext2D | null | undefined;

/** Width of `text` in the label font, measured the way cytoscape measures a label: same font
 *  shorthand, same `measureText`, same rounding up.
 *
 *  Nodes are sized from this rather than by `width: 'label'`, because a node needs room for its counter
 *  chip beside the text and cytoscape cannot widen one side alone: `padding` applies to all four, and
 *  trailing spaces are trimmed off the label before it is measured. */
export function labelWidth(text: string): number {
    if (labelContext === undefined) {
        labelContext = typeof document === 'undefined'
            ? null
            : document.createElement('canvas').getContext('2d');
        if (labelContext !== null) {
            labelContext.font = labelFont();
        }
    }
    if (labelContext === null) {
        return Math.ceil(text.length * LABEL_GLYPH_FALLBACK);
    }
    return Math.ceil(labelContext.measureText(text).width);
}

/** Draw order among edges: a traced (colored) edge renders over any plain one. */
const TRACED_EDGE_Z_INDEX = 1;

/** Heat above which a node's text switches to `textOnHeat`: past here the fill is a deep red that
 *  dark text sinks into. */
export const HEAT_TEXT_FLIP = 60;

/** Which palette the diagram is drawn with. The application picks it; the library never reads a
 *  theme off the document. */
export type DiagramTheme = 'light' | 'dark';

export interface DiagramPalette {
    /** The node id, the leading run of a node's text. */
    text: string;
    /** The operator name, the run after the id: present but subordinate to it. */
    textMuted: string;
    /** Both runs, once the node's heat fill has passed `HEAT_TEXT_FLIP`. */
    textOnHeat: string;
    /** Node borders. */
    border: string;
    /** Edges, and their arrowheads. */
    edge: string;
    /** Node fill for the lowest and highest value of the selected metric. */
    heatLow: string;
    heatHigh: string;
    /** Fill of an expanded region, drawn at `REGION_OPACITY`. */
    region: string;
    /** Edges reachable from the hovered node, and edges that reach it. */
    edgeForward: string;
    edgeBackward: string;
    /** Corner chips: background, outline, and glyph. */
    chipFill: string;
    chipBorder: string;
    chipInk: string;
    /** Minimap: the viewport outline, the frame, and the ink the circuit is drawn in. The ink is a gray,
     *  so the outline shows over it. */
    navigatorViewport: string;
    navigatorGraph: string;
    navigatorInk: string;
}

export const DIAGRAM_PALETTES: Record<DiagramTheme, DiagramPalette> = {
    light: {
        text: '#000000',
        // surface-800 of the Feldera theme, the light half of `text-surface-800-200`
        textMuted: '#585858',
        textOnHeat: '#ffffff',
        border: '#c7ccd4',
        edge: '#5b6169',
        heatLow: '#ffffff',
        heatHigh: '#ff0000',
        // tertiary-200 of the Feldera theme, the light half of `bg-tertiary-200-800`
        region: '#8cabfa',
        edgeForward: '#ff0000',
        edgeBackward: '#0000ff',
        chipFill: '#ffffff',
        chipBorder: '#c7ccd4',
        chipInk: '#2f353c',
        navigatorViewport: '#000000',
        navigatorGraph: '#a9a9a9',
        navigatorInk: '#585858',
    },
    dark: {
        text: '#e8eaed',
        // surface-200 of the Feldera theme, the dark half of `text-surface-800-200`
        textMuted: '#c6c6c6',
        textOnHeat: '#ffffff',
        border: '#454b52',
        edge: '#8b929b',
        // The ramp starts at the diagram background, so the lowest value reads as cold rather than
        // as a lighter node.
        heatLow: '#22262b',
        heatHigh: '#e03131',
        // tertiary-800 of the Feldera theme, the dark half of `bg-tertiary-200-800`
        region: '#0f38ad',
        edgeForward: '#ff6b6b',
        edgeBackward: '#74a9ff',
        chipFill: '#2c3137',
        chipBorder: '#454b52',
        chipInk: '#e8eaed',
        navigatorViewport: '#e8eaed',
        navigatorGraph: '#8b929b',
        navigatorInk: '#8b929b',
    },
};

/** Build the cytoscape stylesheet for a palette. Applying a new one restyles and repaints without
 *  re-running the layout, so a theme switch moves nothing. */
export function buildGraphStyle(theme: DiagramTheme): StylesheetJson {
    const p = DIAGRAM_PALETTES[theme];
    return [
        {
            selector: 'node',
            css: {
                'shape': 'round-rectangle',
                'corner-radius': `${NODE_CORNER_RADIUS}`,
                // The label is measured but never drawn: `nodeText.ts` paints the text as two runs,
                // which one label cannot be. Keeping it is what sizes the node to its text.
                'content': 'data(label)',
                'text-opacity': 0,
                'text-valign': 'center',
                'text-halign': 'center',
                'font-size': `${NODE_FONT_SIZE}px`,
                'font-family': NODE_FONT_FAMILY,
                'font-weight': ID_FONT_WEIGHT,
                'height': `${NODE_INNER_HEIGHT}px`,
                'border-color': p.border,
                'border-width': `${NODE_BORDER_WIDTH}px`,
                'border-style': 'solid',
                'padding': `${NODE_PADDING_X}px`,
                // Measured by `labelWidth` rather than by cytoscape, so the room a counter chip needs
                // can be added to it. An expanded region ignores this and its height both, and sizes
                // itself to its children.
                'width': 'data(text_width)',
                // The code chip is drawn above the node box, and the render cache clips to the
                // bounding box.
                'bounds-expansion': CHIP_BOUNDS_EXPANSION,
                ...CHIP_BACKGROUND_STYLE,
            }
        },
        {
            // Any composite, collapsed or expanded. `has_children` comes from the node definition, so
            // both hold from the first paint. A collapsed one is a row taller, for its counter chip; an
            // expanded one ignores `height` and sizes itself to its children.
            selector: 'node[?has_children]',
            css: {
                'corner-radius': `${COMPOSITE_CORNER_RADIUS}`,
                'height': `${COMPOSITE_INNER_HEIGHT}px`,
            }
        },
        {
            selector: 'node[value]',
            css: {
                'background-color': `mapData(value, 0, 100, ${p.heatLow}, ${p.heatHigh})`,
            }
        },
        {
            // Only the root node is invisible.
            selector: 'node[invisible]',
            style: {
                'display': 'none'
            }
        },
        {
            // Currently unused.
            selector: 'node[hidden]',
            style: {
                'border-style': 'dotted',
                'border-color': p.border,
                'border-width': '1px',
                'width': 'label',
            }
        },
        {
            // An expanded region. The fill is unconditional rather than keyed on a data attribute,
            // which would only take hold at the next graph update.
            selector: ':parent',
            css: {
                'shape': 'round-rectangle',
                'corner-radius': `${REGION_CORNER_RADIUS}`,
                'border-style': 'dashed',
                'padding': `${REGION_PADDING}`,
                // A parent is sized by its children and ignores its own label, so a region holding
                // narrow nodes needs a floor under its width for its name to fit: see `regionSize.ts`.
                // All of the extra width goes to the right, which is the side ELK reserves it on.
                'min-width': 'data(min_width)',
                'min-width-bias-left': '0%',
                'min-width-bias-right': '100%',
                'background-color': p.region,
                'background-opacity': REGION_OPACITY,
            }
        },
        {
            // Taxi routing keeps edges in the gaps between the stacked ELK layers, so they read as a
            // flow rather than as chords across nodes. `vertical` picks up or down per edge, matching
            // the DOWN layout direction.
            selector: 'edge',
            css: {
                // Every edge draws beneath every node, whatever its nesting depth. By default an edge
                // only sinks below nodes at its own depth, which leaves an edge between two operators
                // over the region around them and over the region's chips.
                'z-compound-depth': 'bottom',
                'curve-style': 'round-taxi',
                'taxi-direction': 'vertical',
                'taxi-turn': '50%',
                'taxi-turn-min-distance': 6,
                'taxi-radius': 100,
                'target-arrow-shape': 'triangle',
                'line-color': p.edge,
                'target-arrow-color': p.edge,
                'width': 2
            }
        },
        {
            selector: 'edge.highlight-backward',
            style: {
                'line-color': p.edgeBackward,
                'target-arrow-color': p.edgeBackward,
                'width': 3,
                'z-index': TRACED_EDGE_Z_INDEX
            }
        },
        {
            selector: 'edge.highlight-forward',
            style: {
                'line-color': p.edgeForward,
                'target-arrow-color': p.edgeForward,
                'width': 3,
                'z-index': TRACED_EDGE_Z_INDEX
            }
        },
    ];
}
