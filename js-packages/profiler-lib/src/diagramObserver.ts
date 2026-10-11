// The interface for code that reacts to events in the diagram's lifecycle.
//
// `CytographRendering` controls the diagram: it keeps the graph, updates it, runs the layout, sets the
// colors and gives information about nodes. Some features only need to know when these events occur,
// for example to move the view when a layout is finished, or to hide the diagram while a layout runs.
// Each such feature is a `DiagramObserver` in its own file, with its own state. `CytographRendering`
// calls it only through the hooks below.
//
// The hooks return nothing: `CytographRendering` tells the observers about events and gets no data
// back from them.

import type { DiagramTheme } from './diagramTheme.js';
import type { NodeId } from './profile.js';

/** All hooks are optional. An observer implements only the hooks it needs. */
export interface DiagramObserver {
    /** The graph is about to change: elements are added or removed, then a layout runs. */
    graphWillChange?(): void;
    /** A layout is finished and all nodes are at their final positions. */
    layoutSettled?(): void;
    /** `layout(...).run()` threw, so `layoutSettled` is not called for the last graph change. */
    layoutFailed?(): void;
    /** The user expanded or collapsed a circuit region. The layout for it has not run yet. */
    circuitRegionToggled?(node: NodeId): void;
    /** The theme changed. No node moves. */
    themeChanged?(theme: DiagramTheme): void;
    /** The diagram is about to be destroyed. Cytoscape is still available. */
    dispose?(): void;
}

/** Call `hook` on each observer, in array order. If an observer throws, the error is logged and the
 *  next observers are still called. The observers are independent, so one failure must not stop the
 *  others. */
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
