// An interface for observers of the diagram's lifecycle, for behaviours that react to it.
//
// `CytographRendering` runs the diagram: it holds the graph, diffs it, lays it out, colors it and answers
// questions about nodes. A few behaviours only need to be told when those things happen - where the view
// belongs once a layout finishes, what covers the screen while one is computed. Each of those is a
// `DiagramObserver`: its own file, its own state, reached only through the hooks below.
//
// Data flows one way: `CytographRendering` tells the observers what happened, and never reads anything
// back from them. What an observer does with a hook is its own business, so the diagram cannot come to
// depend on it. They are notified in the order `CytographRendering` lists them, which matters at
// `layoutSettled`: the view settles the zoom and the pan before the picture held over the layout comes
// down onto it.

import type { DiagramTheme } from './diagramTheme.js';
import type { NodeId } from './profile.js';

/** Every hook is optional; an observer implements the ones it has an opinion about. */
export interface DiagramObserver {
    /** The graph is about to be rebuilt: elements added or removed, then a layout run over them. */
    graphWillChange?(): void;
    /** A layout has finished and every node is in its final position. */
    layoutSettled?(): void;
    /** A layout could not be started, so no `layoutSettled` follows the graph change before it. */
    layoutFailed?(): void;
    /** The user expanded or collapsed a composite. The layout for it has not run yet. */
    compositeToggled?(node: NodeId): void;
    /** The palette changed. Nothing moves. */
    themeChanged?(theme: DiagramTheme): void;
    /** The diagram is going away, while cytoscape is still alive. */
    dispose?(): void;
}

/** Tell every observer, in the order given, that something happened. One that throws does not cost the
 *  rest their turn: the observers know nothing about each other, and the one notified last holds a
 *  picture over the diagram that only its own `layoutSettled` takes down. */
export function notifyObservers(
    observers: Iterable<DiagramObserver>,
    hook: (observer: DiagramObserver) => void
): void {
    for (const observer of observers) {
        try {
            hook(observer);
        } catch (e) {
            console.error('A diagram observer threw', e);
        }
    }
}
