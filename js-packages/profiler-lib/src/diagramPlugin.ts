// The diagram's lifecycle, for behaviour that reacts to it rather than drives it.
//
// `CytographRendering` runs the diagram: it holds the graph, diffs it, lays it out, colors it and answers
// questions about nodes. A few behaviours only need to be told when those things happen - where the view
// belongs once a layout finishes, what covers the screen while one is computed. Each of those is a
// `DiagramPlugin`: its own file, its own state, reached only through the hooks below.
//
// Plugins are called in the order `CytographRendering` lists them, which is load bearing at
// `layoutSettled`: the viewport moves before the frozen picture comes down, so the picture is never
// lifted off a view that is about to jump.
//
// Not every collaborator belongs here. The render-side pieces (`installNodeShadows`,
// `installNodeText`, `installChipButtons`) hook cytoscape's own renderer and events and need nothing
// from this lifecycle. Anything that produces data the diagram or its consumers read back - node
// tooltips, metric coloring, the graph diff itself - is not a plugin either: a one-way hook cannot
// carry an answer.

import type { DiagramTheme } from './diagramTheme.js';
import type { NodeId } from './profile.js';

/** Every hook is optional; a plugin implements the ones it has an opinion about. */
export interface DiagramPlugin {
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
