// The minimap: a picture of the whole circuit, the viewport marked on it, and a press or a drag moving
// the view to where it points.
//
// The map is the graph's bounding box and nothing else, so one point on the map stays one point in the
// circuit for as long as the layout does - which is what makes it draggable: the view moves by the
// distance the drag looks like it should move it. A view panned out past the circuit is clipped.
//
// Two surfaces. The picture is a canvas, painted once per layout and again on a palette change (see
// `minimapPicture.ts`). The viewport is a DOM element over it, so a pan or a zoom costs four style
// writes and no repaint.

import type { BoundingBox12, Core } from 'cytoscape';
import { DIAGRAM_PALETTES, type DiagramTheme } from './diagramTheme.js';
import { graphPicture, paintPicture, type GraphPicture } from './minimapPicture.js';
import { Point } from './planar.js';

/** Gap between the picture and the frame, in CSS pixels. Keeps a dot on the edge of the circuit off the
 *  frame, and leaves room for a view panned a little past it. */
const FRAME_PADDING = 8;

/** Side of the map for a square circuit, in CSS pixels. */
const BASELINE_SIDE = 100;
/** Pixels every map gets, whatever its shape: a circuit twice as wide as it is tall is drawn twice as
 *  wide as it is tall, at the same area as a square one. */
const MAP_AREA = BASELINE_SIDE * BASELINE_SIDE;
/** Cap on either side, reached past sixteen to one, where a map at full area would cover the pane it
 *  floats over. There the shape is kept and the area gives way. */
const MAX_SIDE = 4 * BASELINE_SIDE;

/** Displays the graph and the viewport within it, in a rectangular page. */
export class ViewNavigator {
    private readonly root: HTMLDivElement;
    /** Holds the picture and the outline, inside the frame's padding, so both are placed against the
     *  graph's own box. */
    private readonly map: HTMLDivElement;
    /** The picture of the circuit. */
    private readonly canvas: HTMLCanvasElement;
    /** Outline of the viewport, over the picture. */
    private readonly view: HTMLDivElement;

    private theme: DiagramTheme;
    private picture: GraphPicture | null = null;
    /** Map pixels per model unit; zero until there is a picture. */
    private scale = 0;
    private moveTo: (point: Point) => void = () => { };

    /** Build a navigator as a child of the specified parent element. */
    constructor(parent: HTMLElement, theme: DiagramTheme = 'light') {
        this.theme = theme;
        const palette = DIAGRAM_PALETTES[theme];
        this.root = document.createElement('div');
        this.root.id = 'navigator';
        this.root.style.position = 'relative';
        // Clips the viewport outline where it runs past the map.
        this.root.style.overflow = 'hidden';
        this.root.style.border = `1px solid ${palette.navigatorGraph}`;
        this.root.style.padding = `${FRAME_PADDING}px`;
        this.root.style.cursor = 'pointer';
        // A drag across it moves the view; it must not select or scroll.
        this.root.style.touchAction = 'none';
        this.root.style.userSelect = 'none';
        this.root.title = 'Drag to move the view, double click to fit the whole circuit';
        // delete existing children
        parent.innerHTML = '';
        parent.appendChild(this.root);

        this.map = document.createElement('div');
        this.map.style.position = 'relative';
        this.root.appendChild(this.map);

        this.canvas = document.createElement('canvas');
        this.canvas.style.display = 'block';
        this.map.appendChild(this.canvas);

        this.view = document.createElement('div');
        this.view.id = 'navigator-viewport';
        this.view.style.position = 'absolute';
        this.view.style.boxSizing = 'border-box';
        this.view.style.border = `2px solid ${palette.navigatorViewport}`;
        this.view.style.backgroundColor = 'transparent';
        // Presses belong to the map underneath.
        this.view.style.pointerEvents = 'none';
        // In the map, so `showView` can place it in the graph's own coordinates. It may reach out over
        // the frame's padding, where the frame clips it.
        this.map.appendChild(this.view);

        this.followPointer();
    }

    /** Recolor and repaint for a different palette. */
    setTheme(theme: DiagramTheme) {
        this.theme = theme;
        const palette = DIAGRAM_PALETTES[theme];
        this.root.style.borderColor = palette.navigatorGraph;
        this.view.style.borderColor = palette.navigatorViewport;
        this.paint();
    }

    setOnDoubleClick(handler: () => void) {
        this.root.ondblclick = handler;
    }

    /** Handler for a press or a drag on the map, taking the model point pointed at. */
    setOnMoveTo(handler: (point: Point) => void) {
        this.moveTo = handler;
    }

    /** Redraw the picture, and with it the mapping the map stands on. One pass over every element: for
     *  when a layout finishes, not for a frame. */
    showGraph(cy: Core) {
        this.picture = graphPicture(cy);
        this.resize();
        this.paint();
    }

    /** Mark where the viewport is, in model coordinates - `cy.extent()`. */
    showView(extent: BoundingBox12) {
        if (this.picture === null) {
            return;
        }
        const origin = this.picture.box.origin;
        this.view.style.left = `${(extent.x1 - origin.x) * this.scale}px`;
        this.view.style.top = `${(extent.y1 - origin.y) * this.scale}px`;
        this.view.style.width = `${(extent.x2 - extent.x1) * this.scale}px`;
        this.view.style.height = `${(extent.y2 - extent.y1) * this.scale}px`;
    }

    /** Size the map: the shape of the graph's bounding box, at `MAP_AREA`. */
    private resize(): void {
        const box = this.picture?.box;
        const { width, height } = box === undefined
            ? { width: 0, height: 0 }
            : mapSize(box.size.w / box.size.h);
        // One scale for both axes: the map has the shape of the box, so either ratio gives the same
        // number.
        this.scale = box === undefined ? 0 : width / box.size.w;
        this.map.style.width = `${width}px`;
        this.map.style.height = `${height}px`;
        this.canvas.style.width = `${width}px`;
        this.canvas.style.height = `${height}px`;
        // Scale the minimap by the pixel ratio, so a hairline lands on a whole screen pixel.
        const ratio = window.devicePixelRatio || 1;
        this.canvas.width = Math.round(width * ratio);
        this.canvas.height = Math.round(height * ratio);
    }

    private paint(): void {
        const context = this.canvas.getContext('2d');
        if (context === null) {
            return;
        }
        context.setTransform(1, 0, 0, 1, 0, 0);
        context.clearRect(0, 0, this.canvas.width, this.canvas.height);
        if (this.picture === null) {
            return;
        }
        const ratio = window.devicePixelRatio || 1;
        context.setTransform(ratio, 0, 0, ratio, 0, 0);
        paintPicture(context, this.picture, this.scale, this.theme);
    }

    /** Move the view to wherever the pointer is, on a press and for as long as it is held down. The
     *  press centers the view on the point: a minimap is aimed at, and most presses land nowhere near
     *  the outline.
     *
     *  The drag is followed on the window, so it survives the pointer leaving a map a hundred pixels
     *  wide. The diagram behind it sees none of it: cytoscape drops any event whose target is outside
     *  its own container. */
    private followPointer(): void {
        const move = (event: PointerEvent): void => this.pointAt(event);
        const release = (): void => {
            window.removeEventListener('pointermove', move);
            window.removeEventListener('pointerup', release);
            window.removeEventListener('pointercancel', release);
        };
        this.root.addEventListener('pointerdown', (event) => {
            // Or the browser makes the press the start of a text selection or an image drag.
            event.preventDefault();
            window.addEventListener('pointermove', move);
            window.addEventListener('pointerup', release);
            window.addEventListener('pointercancel', release);
            this.pointAt(event);
        });
    }

    /** Report the model point under the pointer, clamped to the graph: there is nothing outside it to
     *  aim at. */
    private pointAt(event: PointerEvent): void {
        if (this.picture === null) {
            return;
        }
        const box = this.picture.box;
        const rect = this.canvas.getBoundingClientRect();
        const clamp = (value: number, low: number, high: number): number =>
            Math.min(Math.max(value, low), high);
        this.moveTo(new Point(
            clamp(box.origin.x + (event.clientX - rect.left) / this.scale,
                box.origin.x, box.bottomRight().x),
            clamp(box.origin.y + (event.clientY - rect.top) / this.scale,
                box.origin.y, box.bottomRight().y)));
    }
}

/** The map for a circuit of this aspect ratio, in CSS pixels: `MAP_AREA` of them in that shape, unless a
 *  side would pass `MAX_SIDE`. */
export function mapSize(aspect: number): { width: number, height: number } {
    const width = Math.sqrt(MAP_AREA * aspect);
    const height = Math.sqrt(MAP_AREA / aspect);
    const over = Math.max(width, height) / MAX_SIDE;
    return over > 1 ? { width: width / over, height: height / over } : { width, height };
}
