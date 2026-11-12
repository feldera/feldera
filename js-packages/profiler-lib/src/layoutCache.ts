// Keeps the results of recent ELK layouts, so that a graph that was already laid out does not go
// through ELK again. This occurs when the user switches between snapshots of one circuit. After an
// expand and a collapse, the edges are often in a different order, so the same region toggles hit the
// cache only some of the time.
//
// The key is the full input of ELK, so a cache hit gives the positions that ELK would give. ELK is
// deterministic, and the `layered` algorithm does not use the input positions of the nodes with the
// options in `cytograph.ts`.

import type { Core, NodeSingular, Position } from 'cytoscape';

/** The options of an ELK layout, as `CytographRendering` gives them to `cy.layout`. */
export interface ElkLayoutOptions {
    nodeLayoutOptions?: (node: NodeSingular) => Record<string, string>;
    nodeDimensionsIncludeLabels?: boolean;
    elk?: Record<string, string | number>;
    [option: string]: unknown;
}

/** The position of each leaf node, by node id. Cytoscape places a parent node around its children. */
export type LeafPositions = Record<string, Position>;

/** A string that is equal for two layouts only if ELK gets the same input for them: the same nodes in
 *  the same order, with the same parents, sizes and node options, the same edges in the same order,
 *  and the same layout options. The order is part of the key, because a different order of the edges
 *  gives a different layout. */
export function layoutCacheKey(cy: Core, options: ElkLayoutOptions): string {
    const nodes = cy.nodes().map((node) => {
        const parent = node.data('parent') ?? null;
        const nodeOptions = options.nodeLayoutOptions?.(node) ?? null;
        if (node.isParent()) {
            return [node.id(), parent, nodeOptions];
        }
        // The same size that `cytoscape-elk` gives to ELK.
        const { w, h } = node.layoutDimensions(options as never);
        return [node.id(), parent, nodeOptions, w, h];
    });
    const edges = cy.edges().map((edge) => [edge.id(), edge.source().id(), edge.target().id()]);
    // `cytoscape-elk` sets the ELK option `aspectRatio` to the aspect ratio of the container. ELK uses it
    // to arrange the unconnected parts of the graph, but not with `INCLUDE_CHILDREN`. Before the first
    // layout the container can have no size yet, so a key with the aspect ratio would not match later.
    const usesAspectRatio = options.elk?.['elk.hierarchyHandling'] !== 'INCLUDE_CHILDREN';
    const aspectRatio = usesAspectRatio ? cy.width() / cy.height() : null;
    // `JSON.stringify` leaves out the functions in `options`. `nodes` has their results.
    return JSON.stringify([options, aspectRatio, nodes, edges]);
}

/** The current positions of all leaf nodes. */
export function leafPositions(cy: Core): LeafPositions {
    const positions: LeafPositions = {};
    for (const node of cy.nodes().toArray()) {
        if (!node.isParent()) {
            positions[node.id()] = { ...node.position() };
        }
    }
    return positions;
}

/** Move the leaf nodes to `positions`. Emits the same events as an ELK layout. */
export function runPresetLayout(cy: Core, positions: LeafPositions): void {
    cy.nodes()
        .filter((node) => !node.isParent())
        .layout({
            name: 'preset',
            positions,
            fit: false,
            animate: false
        })
        .run();
}

/** A cache of layouts that removes the least recently used entry when it is full. */
export class LayoutCache {
    private readonly entries = new Map<string, LeafPositions>();

    constructor(private readonly capacity: number) { }

    get(key: string): LeafPositions | undefined {
        const positions = this.entries.get(key);
        if (positions !== undefined) {
            // A `Map` iterates in insertion order. Insert the entry again to make it the newest.
            this.entries.delete(key);
            this.entries.set(key, positions);
        }
        return positions;
    }

    set(key: string, positions: LeafPositions): void {
        this.entries.delete(key);
        this.entries.set(key, positions);
        if (this.entries.size > this.capacity) {
            const oldest = this.entries.keys().next().value!;
            this.entries.delete(oldest);
        }
    }

    clear(): void {
        this.entries.clear();
    }
}

/** Shared by all diagrams, so that a new diagram of the same graph, for example after a switch to
 *  another snapshot, uses the layout of the old diagram. */
export const layoutCache = new LayoutCache(8);

/** Forget all layouts, so that the next layout runs ELK. For performance tests that time ELK. */
export function clearLayoutCache(): void {
    layoutCache.clear();
}
