// An interface for observers of the diagram's lifecycle, for behaviours that react to it.
//
// `CytographRendering` runs the diagram: it holds the graph, diffs it, lays it out, colors it and answers
// questions about nodes. A few behaviours only need to be told when those things happen - where the view
// belongs once a layout finishes, what covers the screen while one is computed. Each of those is a
// `DiagramObserver`: its own file, its own state, reached only through the hooks below.
//
// Observers have one-way data flow: they are notified in the order `CytographRendering` lists them,
// which matters at `layoutSettled`.

import type { DiagramTheme } from './diagramTheme.js';
import type { NodeId } from './profile.js';

/** Every hook is optional; an observer implements the ones it has an opinion about. */
export interface DiagramObserver {
    /** The graph is about to be rebuilt: elements added or removed, then a layout run over them. */
    graphWillChange?(): void;
    /** A layout has finished and every node is in its final position. */
    layoutSettled?(): void;
    /** The user expanded or collapsed a composite. The layout for it has not run yet. */
    compositeToggled?(node: NodeId): void;
    /** The palette changed. Nothing moves. */
    themeChanged?(theme: DiagramTheme): void;
    /** The diagram is going away, while cytoscape is still alive. */
    dispose?(): void;
}
