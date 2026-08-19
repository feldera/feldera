// What the user looks at while a layout is computed.
//
// A layout is not instant and the graph is rebuilt before it runs, so the diagram would blink out of
// existence on every expand or collapse. This holds the last one on screen instead: the current
// viewport is painted into an overlay above cytoscape's canvases, the canvases are hidden behind it,
// and the overlay comes down once they hold the finished layout.
//
// A `DiagramPlugin` (see `diagramPlugin.ts`), which is all this needs to be: the two hooks it answers
// bracket exactly the window it covers.

import type { Core } from 'cytoscape';
import type { DiagramPlugin } from './diagramPlugin.js';

export class FrozenLayout implements DiagramPlugin {
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

    /** Paint the current viewport into an overlay above cytoscape's own canvases and hide those behind
     *  it. A second freeze keeps the first picture: by then the graph behind it has already changed, so
     *  painting it again would paint a half-updated diagram. */
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
