// The minimap: a picture of the whole circuit, with an outline of the viewport. A `pointerdown` or a
// drag on the minimap moves the view to that point.
//
// The minimap shows only the bounding box of the graph. So the mapping from minimap pixels to graph
// coordinates changes only when the layout changes. This makes a drag move the view by the distance
// that the user expects. When the view is panned past the circuit, the outline is clipped.
//
//   Part       Element   Updated
//   picture    canvas    after each layout and on a theme change (see `minimapPicture.ts`)
//   viewport   div       on each pan or zoom: four style changes, no repaint

import type { BoundingBox12, Core } from 'cytoscape';
import { DIAGRAM_PALETTES, type DiagramTheme } from './diagramTheme.js';
import { graphPicture, paintPicture, type GraphPicture } from './minimapPicture.js';
import { Point } from './planar.js';

/** The gap between the picture and the frame, in CSS pixels. It keeps an operator at the edge of the
 *  circuit away from the frame, and leaves space for a view that is panned a little past the
 *  circuit. */
const FRAME_PADDING = 8;

/** The side of the minimap for a square circuit, in CSS pixels. */
const BASELINE_SIDE = 100;
/** The area of every minimap, for any shape. For example, a circuit that is twice as wide as it is tall
 *  gets a minimap that is twice as wide as it is tall, with the same area as a square minimap. */
const MAP_AREA = BASELINE_SIDE * BASELINE_SIDE;
/** The maximum length of a side. A circuit with an aspect ratio above 16:1 reaches it, because a larger
 *  minimap would cover too much of the diagram. Then `mapSize` keeps the shape and reduces the area. */
const MAX_SIDE = 4 * BASELINE_SIDE;

/** Displays the graph and the viewport within it, in a rectangular page. */
export class ViewNavigator {
    private readonly root: HTMLDivElement;
    /** Contains the picture and the outline, inside the frame padding. Both are positioned relative to
     *  the bounding box of the graph. */
    private readonly map: HTMLDivElement;
    /** The picture of the circuit. */
    private readonly canvas: HTMLCanvasElement;
    /** The outline of the viewport, above the picture. */
    private readonly view: HTMLDivElement;

    private theme: DiagramTheme;
    private picture: GraphPicture | null = null;
    /** Minimap pixels per model unit. Zero until there is a picture. */
    private scale = 0;
    private moveTo: (point: Point) => void = () => { };

    /** Build a navigator as a child of the specified parent element. */
    constructor(parent: HTMLElement, theme: DiagramTheme = 'light') {
        this.theme = theme;
        const palette = DIAGRAM_PALETTES[theme];
        this.root = document.createElement('div');
        this.root.id = 'navigator';
        this.root.style.position = 'relative';
        // Clip the viewport outline where it goes past the minimap.
        this.root.style.overflow = 'hidden';
        this.root.style.border = `1px solid ${palette.navigatorGraph}`;
        this.root.style.padding = `${FRAME_PADDING}px`;
        this.root.style.cursor = 'pointer';
        // A drag on the minimap moves the view. It must not select text or scroll the page.
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
        // Pointer events go to the minimap below the outline.
        this.view.style.pointerEvents = 'none';
        // Put the outline in `map`, so that `showView` can position it in graph coordinates. The
        // outline can go into the frame padding, where the frame clips it.
        this.map.appendChild(this.view);

        this.followPointer();
    }

    /** Change the colors and repaint for `theme`. */
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

    /** Set the handler for a `pointerdown` or a drag on the minimap. It gets the model point under the
     *  pointer. */
    setOnMoveTo(handler: (point: Point) => void) {
        this.moveTo = handler;
    }

    /** Read the graph again, and update the picture and the scale. This reads every element, so call it
     *  when a layout is finished, not on each frame. */
    showGraph(cy: Core) {
        this.picture = graphPicture(cy);
        this.resize();
        this.paint();
    }

    /** Move the outline to `extent`, the viewport in model coordinates (`cy.extent()`). */
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

    /** Set the minimap size: the shape of the bounding box of the graph, with the area `MAP_AREA`. */
    private resize(): void {
        const box = this.picture?.box;
        const { width, height } = box === undefined
            ? { width: 0, height: 0 }
            : mapSize(box.size.w / box.size.h);
        // Use one scale for both axes. The minimap has the shape of the box, so the two ratios are
        // equal.
        this.scale = box === undefined ? 0 : width / box.size.w;
        this.map.style.width = `${width}px`;
        this.map.style.height = `${height}px`;
        this.canvas.style.width = `${width}px`;
        this.canvas.style.height = `${height}px`;
        // Scale the canvas by the device pixel ratio, so that a 1 px line covers a whole screen pixel.
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

    /** From `pointerdown` until `pointerup` or `pointercancel`, center the view on the point under the
     *  pointer. The view jumps to the point instead of following the drag distance, because the user
     *  usually points at a place on the minimap, not at the outline.
     *
     *  The `pointermove` listener is on the window, so the drag continues when the pointer leaves the
     *  small minimap. The diagram does not get these events, because cytoscape ignores events whose
     *  target is outside its container. */
    private followPointer(): void {
        const move = (event: PointerEvent): void => this.pointAt(event);
        const release = (): void => {
            window.removeEventListener('pointermove', move);
            window.removeEventListener('pointerup', release);
            window.removeEventListener('pointercancel', release);
        };
        this.root.addEventListener('pointerdown', (event) => {
            // Otherwise the browser starts a text selection or an image drag.
            event.preventDefault();
            window.addEventListener('pointermove', move);
            window.addEventListener('pointerup', release);
            window.addEventListener('pointercancel', release);
            this.pointAt(event);
        });
    }

    /** Send the model point under the pointer to the handler. The point is clamped to the bounding box
     *  of the graph, because there is nothing outside it. */
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

/** The minimap size for a circuit with this aspect ratio, in CSS pixels. The area is `MAP_AREA`, unless
 *  a side would be longer than `MAX_SIDE`. */
export function mapSize(aspect: number): { width: number, height: number } {
    const width = Math.sqrt(MAP_AREA * aspect);
    const height = Math.sqrt(MAP_AREA / aspect);
    const over = Math.max(width, height) / MAX_SIDE;
    return over > 1 ? { width: width / over, height: height / over } : { width, height };
}
