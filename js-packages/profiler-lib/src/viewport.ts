// Where the view is: the zoom a node is read at, where a profile opens, what a search moves to, what
// happens to a composite the layout moved off screen, and the minimap that reports all of it.
//
// A `DiagramPlugin` (see `diagramPlugin.ts`), so the policy lives here rather than as four flags on
// `CytographRendering`: the diagram tells it a layout finished, a composite was toggled or the palette
// changed, and everything the view does follows from those three plus the two requests it takes
// directly, `center` and `centerOnNextLayout`.

import type { Core, NodeSingular, Position } from 'cytoscape';
import type { DiagramPlugin } from './diagramPlugin.js';
import { NODE_INNER_HEIGHT, type DiagramTheme } from './diagramTheme.js';
import { ViewNavigator } from './navigator.js';
import type { NodeId } from './profile.js';
import { Option } from './util.js';

export class Viewport implements DiagramPlugin {
    /** Rendered height a node's own box is brought to when the view moves to it. Measured on the box
     *  cytoscape's `renderedHeight` reports, which is the node without the padding around it. */
    private static readonly FOCUS_NODE_HEIGHT = 7.5;
    /** The zoom that gives a node that height: where a profile opens, and where a search stops zooming
     *  in. One number for both, so opening a profile and finding a node in it leave the diagram at the
     *  same scale. */
    private static readonly FOCUS_ZOOM = Viewport.FOCUS_NODE_HEIGHT / NODE_INNER_HEIGHT;

    /** Minimap of the graph and of this viewport within it. */
    readonly navigator: ViewNavigator;

    /** False until the first layout has placed the view; every layout after that leaves it alone. */
    private placed = false;
    /** A node to center on when the layout in flight finishes, set by `centerOnNextLayout`. */
    private requested: Option<NodeId> = Option.none();
    /** A composite just expanded or collapsed, to be brought back on screen if the layout that
     *  follows puts it outside the viewport. */
    private toggled: Option<NodeId> = Option.none();

    /** @param firstNode The node a profile opens on, asked for when the first layout finishes. */
    constructor(
        private readonly cy: Core,
        navigatorContainer: HTMLElement,
        private readonly firstNode: () => NodeId | undefined,
        theme: DiagramTheme
    ) {
        this.navigator = new ViewNavigator(navigatorContainer, theme);
        this.navigator.setOnDoubleClick(() => this.cy.fit());
        this.navigator.setOnMoveTo((point) => this.panTo(point));
        this.cy.on('zoom pan resize', () => this.syncNavigator());
    }

    /** Center the view on this node once the layout that is about to run has finished. */
    centerOnNextLayout(node: Option<NodeId>): void {
        this.requested = node;
    }

    /** Move to a node now: centered, and zoomed in to `FOCUS_NODE_HEIGHT` if it is smaller than that.
     *  Never zooms out - a view already closer in than the focus height stays there. */
    center(id: NodeId): void {
        const el = this.cy.getElementById(id);
        if (!el.nonempty()) {
            return;
        }
        // The node's height is what the zoom is chosen by, being the dimension tied to the font size.
        const size = el.renderedHeight();
        if (size < Viewport.FOCUS_NODE_HEIGHT) {
            this.cy.zoom({
                level: this.cy.zoom() * Viewport.FOCUS_NODE_HEIGHT / size,
                position: el.position()
            });
        }
        this.centerOn(el);
    }

    compositeToggled(node: NodeId): void {
        this.toggled = Option.some(node);
    }

    themeChanged(theme: DiagramTheme): void {
        this.navigator.setTheme(theme);
    }

    layoutSettled(): void {
        // Where the minimap's picture is taken: every node has just landed where it belongs.
        this.navigator.showGraph(this.cy);
        this.syncNavigator();
        // Before any centering below, so that every zoom set here is clamped by them.
        this.clampZoom();
        if (!this.placed) {
            this.placed = true;
            this.placeInitialView();
        }
        if (this.requested.isSome()) {
            // An explicit request to go to a node outranks keeping a toggled one in view.
            this.center(this.requested.unwrap());
            this.requested = Option.none();
        } else if (this.toggled.isSome()) {
            this.revealToggled(this.toggled.unwrap());
        }
        this.toggled = Option.none();
    }

    /** How far in and out the user may go: no closer than a node needs to be legible, no further out
     *  than fitting the whole graph takes. */
    private clampZoom(): void {
        this.cy.maxZoom(1.5);
        const rect = this.cy.container()?.getBoundingClientRect();
        if (rect !== undefined) {
            const bb = this.cy.elements().boundingBox();
            this.cy.minZoom(Math.min(rect.height / bb.h, rect.width / bb.w));
        }
    }

    /** Place the view the first time a profile is laid out: on its first node, at the zoom a search
     *  moves to, since fitting a large circuit on screen leaves nothing on it legible. Every layout after
     *  this one leaves the viewport alone. */
    private placeInitialView(): void {
        const first = this.firstNode();
        if (first === undefined) {
            return;
        }
        this.cy.zoom(Viewport.FOCUS_ZOOM);
        this.centerOn(this.cy.getElementById(first) as NodeSingular);
    }

    /** Pan to the composite the user just expanded or collapsed, but only if the layout that followed put
     *  it outside the viewport. ELK moves every node, so a composite that grew or shrank can end up off
     *  screen, leaving no sign of where the node they pressed went. While any part of it is still visible
     *  the view stays exactly where it was. Only the pan moves; the zoom is the user's. */
    private revealToggled(id: NodeId): void {
        const el = this.cy.getElementById(id);
        if (!el.nonempty()) {
            return;
        }
        // Model coordinates on both sides. The node's own box rather than `boundingBox()`, which
        // reaches above a node to cover its code chip.
        const view = this.cy.extent();
        const position = el.position();
        const halfWidth = el.outerWidth() / 2;
        const halfHeight = el.outerHeight() / 2;
        const onScreen = position.x + halfWidth > view.x1 && position.x - halfWidth < view.x2
            && position.y + halfHeight > view.y1 && position.y - halfHeight < view.y2;
        if (!onScreen) {
            this.centerOn(el);
        }
    }

    /** Pan so that the center of `el` is the center of the view. Not cytoscape's own `center`, which
     *  works off the element's bounding box - that box reaches above a node to cover its code chip, so
     *  centering by it leaves the node itself half a chip off center. */
    private centerOn(el: { position(): Position }): void {
        this.panTo(el.position());
    }

    /** Pan so that this model point is the center of the view. */
    private panTo(point: Position): void {
        const zoom = this.cy.zoom();
        this.cy.pan({
            x: this.cy.width() / 2 - point.x * zoom,
            y: this.cy.height() / 2 - point.y * zoom
        });
    }

    /** Tell the minimap where the view is. Runs on every pan and zoom, so it only moves an outline. */
    private syncNavigator(): void {
        // Cytoscape's resize observer is debounced, so a `resize` fired on the way out arrives after
        // the instance has been destroyed - and a destroyed instance has no renderer left to answer
        // `extent()`.
        if (this.cy.destroyed()) {
            return;
        }
        this.navigator.showView(this.cy.extent());
    }
}
