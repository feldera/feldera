

import type { Core, NodeSingular, Position } from 'cytoscape';
import type { DiagramObserver } from './diagramObserver.js';
import { NODE_FONT_SIZE, type DiagramTheme } from './diagramTheme.js';
import { ViewNavigator } from './navigator.js';
import type { NodeId } from './profile.js';
import { Option } from './util.js';

/**
 * Controls the position of the view on the diagram: the zoom level, where the view starts when a
 * profile opens, where a search moves the view, and the minimap that shows the view. When the user
 * expands or collapses a circuit region, the new layout can move that region off screen. Then this
 * class pans the view back to it.
 */
export class Viewport implements DiagramObserver {
    /** The font size of the node text on screen, in CSS pixels, at `FOCUS_ZOOM`. */
    private static readonly FOCUS_FONT_SIZE = 12;
    /** The zoom level at which the node text is `FOCUS_FONT_SIZE` CSS pixels.
     *  A profile opens at this zoom, and a search does not zoom in more than this.
     *  An exception: if the full graph fits in the view at a higher zoom,
     *  the profile opens at that zoom. */
    private static readonly FOCUS_ZOOM = Viewport.FOCUS_FONT_SIZE / NODE_FONT_SIZE;

    /** Minimap that shows the full graph and the viewport outline. */
    readonly navigator: ViewNavigator;

    /** False until the first layout places the view on the first node. Only the first layout does this. */
    private placed = false;
    /** The node to center on when the current layout finishes. `centerOnNextLayout` sets it. */
    private nodeToCenterOn: Option<NodeId> = Option.none();
    /** The circuit region that the user just expanded or collapsed. If the next layout moves it off
     *  screen, the view pans back to it. */
    private toggled: Option<NodeId> = Option.none();

    /** @param firstNode Gives the node that the view shows when the first layout finishes. */
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

    /** Center the view on this node when the next layout finishes. */
    centerOnNextLayout(node: Option<NodeId>): void {
        this.nodeToCenterOn = node;
    }

    /** Move the view to a node now. Center the node, and if the zoom is less than `FOCUS_ZOOM`, zoom
     *  in to `FOCUS_ZOOM`. This never zooms out: if the view is already closer, the zoom does not
     *  change. */
    center(id: NodeId): void {
        const el = this.cy.getElementById(id);
        if (!el.nonempty()) {
            return;
        }
        if (this.cy.zoom() < Viewport.FOCUS_ZOOM) {
            this.cy.zoom({ level: Viewport.FOCUS_ZOOM, position: el.position() });
        }
        this.centerOn(el);
    }

    circuitRegionToggled(node: NodeId): void {
        this.toggled = Option.some(node);
    }

    themeChanged(theme: DiagramTheme): void {
        this.navigator.setTheme(theme);
    }

    layoutSettled(): void {
        // Update the minimap here, because all nodes are now at their final positions.
        this.navigator.showGraph(this.cy);
        this.syncNavigator();
        // Set the zoom limits before the code below centers the view, so that the limits apply to
        // each zoom it sets.
        this.clampZoom();
        if (!this.placed) {
            this.placed = true;
            this.placeInitialView();
        }
        if (this.nodeToCenterOn.isSome()) {
            // A request to go to a node has priority over keeping a toggled circuit region in view.
            this.center(this.nodeToCenterOn.unwrap());
            this.nodeToCenterOn = Option.none();
        } else if (this.toggled.isSome()) {
            this.revealToggled(this.toggled.unwrap());
        }
        this.toggled = Option.none();
    }

    /** Set the zoom limits. The user cannot zoom in more than 1.5, and cannot zoom out more than
     *  the zoom that fits the full graph in the view. */
    private clampZoom(): void {
        this.cy.maxZoom(1.5);
        const rect = this.cy.container()?.getBoundingClientRect();
        if (rect !== undefined) {
            const bb = this.cy.elements().boundingBox();
            this.cy.minZoom(Math.min(rect.height / bb.h, rect.width / bb.w));
        }
    }

    /** Place the view after the first layout of a profile: center it on the first node at
     *  `FOCUS_ZOOM`. Do not fit the full circuit in the view, because the text of a large circuit is
     *  then too small to read. */
    private placeInitialView(): void {
        const first = this.firstNode();
        const el = first === undefined ? undefined : this.cy.getElementById(first);
        if (el === undefined || !el.nonempty()) {
            // The circuit has only its root node, so there is no node to open on. The layout does not
            // fit the graph in the view, so this call must do it.
            this.cy.fit();
            return;
        }
        this.cy.zoom(Viewport.FOCUS_ZOOM);
        this.centerOn(el as NodeSingular);
    }

    /** Pan to the circuit region that the user just expanded or collapsed, if the new layout moved
     *  it fully off screen. When a region grows or shrinks, ELK moves all nodes, so the region that
     *  the user clicked can move out of view. If a part of the region is still visible, the view does
     *  not move. This changes only the pan, not the zoom. */
    private revealToggled(id: NodeId): void {
        const el = this.cy.getElementById(id);
        if (!el.nonempty()) {
            return;
        }
        // Both boxes use model coordinates. Use the node box (`outerWidth()` / `outerHeight()`), not
        // `boundingBox()`, because `boundingBox()` also includes the code chip above the node.
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

    /** Pan so that the center of `el` is at the center of the view. We do not use cytoscape's own
     *  `center`, because it uses the bounding box, which includes the code chip above the node. That
     *  puts the node half a chip height off center. */
    private centerOn(el: { position(): Position }): void {
        this.panTo(el.position());
    }

    /** Pan so that this model point is at the center of the view. */
    private panTo(point: Position): void {
        const zoom = this.cy.zoom();
        this.cy.pan({
            x: this.cy.width() / 2 - point.x * zoom,
            y: this.cy.height() / 2 - point.y * zoom
        });
    }

    /** Send the view position to the minimap. This runs on each pan and zoom, so it only moves the
     *  viewport outline. */
    private syncNavigator(): void {
        // Cytoscape delays its resize observer, so a `resize` event can arrive after the instance is
        // destroyed. A destroyed instance has no renderer, so we guard the `extent()` call.
        if (this.cy.destroyed()) {
            return;
        }
        this.navigator.showView(this.cy.extent());
    }
}
