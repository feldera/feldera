// What the user looks at while a layout is computed.
//
// Computing and rendering a layout is not instant, and the graph is rebuilt before it runs, so the
// diagram would go blank on every expand or collapse of a node. This holds the last rendered layout on
// screen instead: the pixels already in cytoscape's canvases are copied into an overlay canvas above
// them, the canvases are hidden behind it, and the overlay comes down once they hold the finished layout.

import type { Core } from 'cytoscape';
import type { DiagramObserver } from './diagramObserver.js';

export class FrozenLayout implements DiagramObserver {
    /** Copy of the layout that is on screen, held over the diagram while the next one is computed. */
    private picture: HTMLCanvasElement | null = null;
    /** False until a layout has been shown; the first one has nothing to hold over. */
    private drawn = false;

    constructor(private readonly cy: Core) { }

    graphWillChange(): void {
        if (this.drawn) {
            this.freeze();
            return;
        }
        // Nothing is drawn yet, so there is nothing to hold: hide the container until the first layout
        // has something worth showing.
        const container = this.cy.container();
        if (container !== null) {
            container.style.visibility = 'hidden';
        }
    }

    layoutSettled(): void {
        this.drawn = true;
        this.reveal();
    }

    /** The layout never started, so no `layoutSettled` is coming for it. Nothing else would take the
     *  picture down, and the user would be left with an image of the diagram that answers nothing: the
     *  overlay takes no pointer events, and the canvases that do are hidden behind it. */
    layoutFailed(): void {
        this.reveal();
    }

    dispose(): void {
        this.reveal();
    }

    /** Copy what is on screen into an overlay canvas, and hide the canvases behind it. A second graph
     *  change before the layout settles keeps the first copy, that being the last layout anyone saw. */
    private freeze(): void {
        const container = this.cy.container();
        // Cytoscape's canvases live in a wrapper of its own making, the one element known to be
        // positioned. A headless instance has no container and nothing drawn to copy.
        const layers = container?.firstElementChild as HTMLElement | null | undefined;
        if (container === null || !layers || this.picture !== null) {
            return;
        }
        const canvases = Array.from(container.querySelectorAll('canvas'));
        const first = canvases[0];
        if (first === undefined) {
            return;
        }
        const picture = container.ownerDocument.createElement('canvas');
        // Sized in device pixels, so the copy is as sharp as what it covers, and stretched back to the
        // container by its style. Cytoscape keeps its layers the same size, so the first measures them all.
        picture.width = first.width;
        picture.height = first.height;
        picture.style.cssText = 'position:absolute;left:0;top:0;width:100%;height:100%;'
            + 'pointer-events:none;z-index:10';
        const context = picture.getContext('2d');
        if (context === null) {
            return;
        }
        // One blit per layer. `cy.png()` would draw every element again and PNG-encode the result on the
        // main thread, in the moment the user asked for the change, and it would cost the most on the
        // circuits whose layouts are slow enough to need freezing in the first place.
        for (const layer of canvases) {
            context.drawImage(layer, 0, 0);
        }
        // Before the picture goes up, so that hiding the layers does not hide the picture with them.
        this.showLayers(canvases, false);
        layers.appendChild(picture);
        this.picture = picture;
    }

    /** Take the picture down, showing the layout that was computed behind it. */
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
