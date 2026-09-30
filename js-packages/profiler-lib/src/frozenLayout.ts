// Keeps the old layout on screen while a new layout is computed.
//
// When the user expands or collapses a circuit region, the new layout needs to be computed,
// which introduces a significant delay. While the new layout is recomputed the diagram canvas
// goes blank. To prevent this, this file copies the pixels of the cytoscape canvases into an
// overlay canvas above them, and hides the cytoscape canvases. When the new layout is finished,
// it removes the copy and shows the cytoscape canvases again.
//
// Before the first layout there is nothing to copy, so this file hides the whole container instead.

import type { Core } from 'cytoscape';
import type { DiagramObserver } from './diagramObserver.js';

export class FrozenLayout implements DiagramObserver {
    /** The copy of the last layout, while a new layout is calculated. `null` when no copy is shown. */
    private picture: HTMLCanvasElement | null = null;
    /** False until the first layout is finished. Before that, there is nothing to copy. */
    private drawn = false;

    constructor(private readonly cy: Core) { }

    graphWillChange(): void {
        if (this.drawn) {
            this.freeze();
            return;
        }
        // There is no layout to copy yet. Hide the container until the first layout is finished.
        const container = this.cy.container();
        if (container !== null) {
            container.style.visibility = 'hidden';
        }
    }

    /** Removes the copy. Put this observer last, so that the `layoutSettled` changes of the other
     *  observers (for example the zoom and pan of `Viewport`) happen while the copy is on screen. */
    layoutSettled(): void {
        this.drawn = true;
        this.reveal();
    }

    /** Called when `layout(...).run()` throws, for example because of bad layout options (see
     *  `CytographRendering.initiateLayout`). No `layoutSettled` follows, so this hook removes the copy.
     *  Otherwise the copy stays on screen and the diagram ignores the mouse: the copy has
     *  `pointer-events: none`, and the cytoscape canvases under it are hidden. */
    layoutFailed(): void {
        this.reveal();
    }

    dispose(): void {
        this.reveal();
    }

    /** Copy the cytoscape canvases into an overlay canvas, and hide the cytoscape canvases. If the
     *  graph changes again before the layout is finished, keep the first copy, because it shows the
     *  last finished layout. */
    private freeze(): void {
        const container = this.cy.container();
        // Cytoscape puts its canvases in a wrapper element with `position: relative`. The copy goes in
        // that element too. A headless instance has no container and no canvases.
        const layers = container?.firstElementChild as HTMLElement | null | undefined;
        if (container === null || !layers || this.picture !== null) {
            return;
        }
        // The cytoscape canvas renderer draws on three stacked canvases (layers), so that it can redraw
        // one of them without the others:
        //
        //   Layer        Draws
        //   node         the elements (bottom layer)
        //   drag         the elements that the user drags
        //   select box   the selection rectangle (top layer)
        const canvases = Array.from(container.querySelectorAll('canvas'));
        const first = canvases[0];
        if (first === undefined) {
            return;
        }
        const picture = container.ownerDocument.createElement('canvas');
        // The canvas size is in device pixels, so the copy is as sharp as the original. The style
        // scales the copy to the container. All cytoscape canvases have the same size, so the first one
        // gives the size.
        picture.width = first.width;
        picture.height = first.height;
        picture.style.cssText = 'position:absolute;left:0;top:0;width:100%;height:100%;'
            + 'pointer-events:none;z-index:10';
        const context = picture.getContext('2d');
        if (context === null) {
            return;
        }
        // Copy the pixels that are already on the canvases. `cy.png()` is slower: it paints every
        // node and edge again on a new canvas, and then encodes it as a PNG.
        for (const layer of canvases) {
            context.drawImage(layer, 0, 0);
        }
        // `canvases` was collected before the copy was made, so this does not hide the copy.
        this.showLayers(canvases, false);
        layers.appendChild(picture);
        this.picture = picture;
    }

    /** Remove the copy, and show the container and the cytoscape canvases. */
    private reveal(): void {
        const container = this.cy.container();
        if (container !== null) {
            container.style.visibility = '';
        }
        this.picture?.remove();
        this.picture = null;
        this.showLayers(Array.from(container?.querySelectorAll('canvas') ?? []), true);
    }

    private showLayers(canvases: Iterable<HTMLCanvasElement>, show: boolean): void {
        for (const canvas of canvases) {
            canvas.style.visibility = show ? '' : 'hidden';
        }
    }
}
