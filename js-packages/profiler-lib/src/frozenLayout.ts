// What the user looks at while a layout is computed.
//
// Calculating and rendering a layout is not instant and the graph is rebuilt before it runs,
// so the diagram would go blank on every expand or collapse of a node. This holds the last rendered layout
// on screen instead: the current viewport is painted into an overlay above cytoscape's canvases,
// the canvases are hidden behind it, and the overlay comes down once they hold the finished layout.

import type { Core } from 'cytoscape';
import type { DiagramObserver } from './diagramObserver.js';

export class FrozenLayout implements DiagramObserver {
    /** Picture of the layout that is on screen, held over the diagram while the next one is computed. */
    private picture: HTMLElement | null = null;
    /** False until a layout has been revealed; the first one has nothing to hold over. */
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
        const container = this.cy.container();
        if (container !== null) {
            container.style.visibility = 'visible';
        }
        this.drawn = true;
        this.thaw();
    }

    dispose(): void {
        this.thaw();
    }

    /** Cover the diagram with a captured image of what is on screen, and hide the canvases behind it. The
     *  screenshot stays up until `layoutSettled` takes it down. A graph change before that freezes
     *  again; repeated calls to freeze() do not capture a new image. */
    private freeze(): void {
        const container = this.cy.container();
        // Cytoscape's own canvases live in a wrapper of its own making, the one element known to be
        // positioned. A headless instance has no container and nothing drawn to freeze.
        const layers = container?.firstElementChild as HTMLElement | null | undefined;
        if (container === null || !layers || this.picture !== null) {
            return;
        }
        const picture = document.createElement('div');
        picture.style.cssText = 'position:absolute;left:0;top:0;width:100%;height:100%;'
            + 'background-repeat:no-repeat;background-size:100% 100%;pointer-events:none;z-index:10';
        const snapshot = this.cy.png({
            output: 'base64uri',
            full: false,
            scale: window.devicePixelRatio || 1
        });
        picture.style.backgroundImage = `url("${snapshot}")`;
        layers.appendChild(picture);
        this.showLayers(false);
        this.picture = picture;
    }

    /** Drop the frozen picture, revealing the layout computed behind it. */
    private thaw(): void {
        this.picture?.remove();
        this.picture = null;
        this.showLayers(true);
    }

    private showLayers(show: boolean): void {
        for (const canvas of this.cy.container()?.querySelectorAll('canvas') ?? []) {
            (canvas as HTMLElement).style.visibility = show ? '' : 'hidden';
        }
    }
}
