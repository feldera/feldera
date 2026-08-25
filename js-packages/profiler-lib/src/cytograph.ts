// A cytograph is a graph representation suitable for the cytoscape graph layout and rendering tool

import cytoscape, { type EdgeCollection, type EdgeDefinition, type ElementsDefinition, type EventObject, type NodeCollection, type NodeDefinition, type NodeSingular } from 'cytoscape';
import dblclick from 'cytoscape-dblclick';
import { assert, Graph, OMap, Option, type EncodableAsString, NumericRange, Edge } from './util.js';
import { categoryShares, CircuitProfile, ComplexNode, NodeAndMetric, PropertyValue, totalShare, type DisplayScales, type NodeId } from './profile.js';
import { CircuitSelection } from './selection.js';
import elk from 'cytoscape-elk';
import { Sources } from './dataflow.js';
import { ViewNavigator } from './navigator.js';
import { ZSet } from "./zset.js";
import { MetadataSelection } from './metadataSelection.js';
import { type NodeAttributes, type TooltipCell, type ProfilerCallbacks } from './profiler.js';
import { buildGraphStyle, type DiagramTheme, labelWidth } from './diagramTheme.js';
import { nodeChips } from './chips.js';
import { installNodeShadows, SELECTED_NODE_CLASS } from './nodeShadow.js';
import { installNodeText } from './nodeText.js';
import { elkNodeLayoutOptions, regionMinWidth } from './regionSize.js';

/** A measurement together with a normalized [0, 100] percentile for color scaling. The original
 * `PropertyValue` is preserved so consumers can format on demand (via `.toString()`) or compute
 * over the raw number (via `.getNumericValue()`). */
class SerializedMeasurement {
    constructor(readonly value: PropertyValue, readonly percentile: number) { }

    toString(): string {
        return this.value.toString();
    }
}

/** A matrix of measurements */
class MeasurementMatrix {
    constructor(
        // There should be one column name for each value in the attributes array
        readonly columnNames: Array<string>,
        // Order that the metrics should be displayed in
        readonly metricOrder: Array<string>,
        // Keys are measurement names, arrays contain one element per column name.
        readonly attributes: Map<string, Array<SerializedMeasurement>>,
        // Values added up over the columns, for the metrics that add up.
        readonly totals: Map<string, SerializedMeasurement> = new Map()) {
        for (const a of attributes.entries()) {
            assert(columnNames.length == a[1].length,
                "Measurement count mismatch for '" + a[0] + "':" + columnNames.length + " vs " + a.length);
        }
    }

    attributeCount(): number {
        return this.attributes.size;
    }

    getAttributes(): Map<string, Array<SerializedMeasurement>> {
        return this.attributes;
    }

    getColumnCount(): number {
        return this.columnNames.length;
    }

    toString(): string {
        let result = "";
        for (const a of this.attributes.entries()) {
            result += a[0] + "=[";
            for (const m of a[1]) {
                if (!result.endsWith("["))
                    result += ",";
                result += m;
            }
            result += "\n";
        }
        return result;
    }
}

/** Attributes attached to a cytoscape node. */
class Attributes {
    constructor(
        // Matrix of measurements
        readonly matrix: MeasurementMatrix,
        // Key-value attributes
        readonly kv: Map<string, string>,
    ) { };
}

/** Cytoscape graph node that corresponds to a visible node from the circuit profile. */
class GraphNode {
    constructor(
        readonly id: NodeId,
        readonly persistentId: Option<string>,
        readonly label: string,
        // True if the node has children
        readonly hasChildren: boolean,
        // True if the node's children are displayed
        readonly expanded: boolean,
        readonly parent: Option<string>,
        // Source position information
        readonly sources: string,
        // Number of primitive operators inside this node; 0 for a primitive operator itself
        readonly leafCount: number = 0) {
    }

    asString(): string {
        return this.id;
    }

    getParent() {
        return this.parent;
    }

    /**
     * @returns true if the node has source position information
     */
    get hasSourcePosition() {
        return this.sources.length > 0
    }

    /** Returns a data structure understood by cytoscape for a node. */
    getDefinition(theme: DiagramTheme): NodeDefinition {
        const label = this.label === '' ? this.id : `${this.id} ${this.label}`;

        let result = {
            // These data attributes can be used in cytoscape.StylesheetJson to conditionally style the nodes.
            // Example: `{ selector: 'node[?has_source]', css: { ...`
            "data": {
                "id": this.id,
                "label": label,
                "text_width": labelWidth(label),
                // For an expanded node its min width depends on the title and the chip button
                "min_width": this.hasChildren ? regionMinWidth(label, this.leafCount) : 0,
                "operator": this.label,
                "sources": this.sources,
                "has_source": this.hasSourcePosition,
                "has_children": this.hasChildren,
                // What the counter chip reports, and what `chipButtons.ts` needs to size the pill it
                // hit-tests.
                "leaf_count": this.leafCount,
                // Corner chips; the stylesheet maps this to `background-image`.
                "chips": nodeChips(this.hasSourcePosition, this.leafCount, theme)
            }
        };
        let data = result["data"] as any;
        if (this.parent.isSome())
            data["parent"] = this.parent.unwrap();
        return result;
    }

    getLabel(): string {
        return this.label;
    }

    getId(): NodeId {
        return this.id;
    }

    getSources(): string {
        return this.sources;
    }
}

/** Cytoscape graph edge.
 * Edge ids should be unique per graph to enable computing graph diffs. */
class GraphEdge implements EncodableAsString {
    constructor(readonly source: string, readonly target: string, readonly id: string, readonly backedge: boolean) { }

    getDefinition(): EdgeDefinition {
        let def: EdgeDefinition = { "data": { "id": this.id, "source": this.source, "target": this.target } };
        if (this.backedge)
            def["classes"] = "back";
        return def;
    }

    getId(): string {
        return this.id;
    }

    asString(): string {
        return this.id;
    }
}

/** A directed graph which can be displayed using the Cytoscape rendering library. */
export class Cytograph {
    // Maps each node to the collection of all its primitive children
    readonly nodeChildren: OMap<NodeId, Set<NodeId>>;
    readonly nodes: Array<GraphNode>;
    readonly edges: Array<GraphEdge>;
    // Maps edges ids in the underlying graph to edges ids in the drawn graph
    readonly edgeMap: Map<string, string>;

    constructor(readonly graph: Graph<NodeId>) {
        this.nodes = [];
        this.edges = [];
        this.edgeMap = new Map();
        this.nodeChildren = new OMap();
    }

    addNode(node: GraphNode) {
        this.nodes.push(node);
    }

    /** Map an edge in the real underlying graph to an edge in the displayed graph. */
    mapEdge(original: GraphEdge, real: GraphEdge) {
        this.edgeMap.set(original.getId(), real.getId());
    }

    /** Get all the simple nodes inside the specified node. */
    getSimpleNodes(id: NodeId): Set<NodeId> {
        if (this.nodeChildren.has(id))
            return this.nodeChildren.get(id).unwrap();
        return new Set([id]);
    }

    /** Create a graph edge, but do not add it to the graph. */
    createEdge(source: string, target: string, back: boolean): GraphEdge {
        // Must generate a unique ID for each edge; source and target and not enough
        // to distinguish two edges, since this is in general a multigraph.
        let identical_count = 0;
        for (const e of this.edges) {
            if (e.source === source && e.target === target) {
                identical_count++;
            }
        }
        let id = source + "->" + target;
        if (identical_count > 0) {
            id += ":" + (identical_count + 1);
        }
        return new GraphEdge(source, target, id, back);
    }

    /** Used to convert nodes and edges to a representation understood by cytoscape. */
    getGraphElements(theme: DiagramTheme): ElementsDefinition {
        return {
            "nodes": this.nodes.map(n => n.getDefinition(theme)),
            "edges": this.edges.map(e => e.getDefinition())
        }
    }

    toString(): string {
        return JSON.stringify(this.nodes) + "\n" + JSON.stringify(this.edges);
    }

    // Build the underlying graph data structure where each profile simple node
    // is a graph node.  Nested nodes are ignored.
    static createUnderlyingGraph(profile: CircuitProfile): Graph<NodeId> {
        let g: Graph<NodeId> = new Graph();
        for (const nodeId of profile.simpleNodes.keys()) {
            g.addNode(nodeId);
        }
        for (const edge of profile.edges) {
            let source = edge.source;
            let target = edge.target;
            let weight = 1;
            if (profile.complexNodes.has(target))
                // Do not add edges to complex nodes.
                continue;
            g.addEdge(source, target, weight, edge.back);
        }
        return g;
    }

    /** Given a metric, return the displayed nodes that have the top values for the metric. */
    topNodes(profile: CircuitProfile, metric: string): Array<NodeAndMetric> {
        const displayed = this.nodes
            .filter(node => !node.expanded)
            .map(node => ({ id: node.getId(), label: node.label }));
        return profile.rankNodes(metric, displayed);
    }

    /** The node drawn in place of `nodeId`: the outermost region around it that is collapsed, or the
     *  node itself when every region around it is expanded. Walking from the outside in is what lets a
     *  region inside an expanded one be collapsed on its own, since collapsing an outer region hides the
     *  inner ones whatever state they are in. */
    static drawnNode(profile: CircuitProfile, selection: CircuitSelection, nodeId: NodeId): NodeId {
        let regions: Array<NodeId> = [];
        let parent = profile.parents.get(nodeId);
        while (parent.isSome()) {
            regions.unshift(parent.unwrap());
            parent = profile.parents.get(parent.unwrap());
        }
        for (const region of regions) {
            if (!selection.regionsExpanded.contains(region)) {
                return region;
            }
        }
        return nodeId;
    }

    // Create a Cytograph from a CircuitProfile filtered by the specified selection.
    static fromProfile(profile: CircuitProfile, selection: CircuitSelection): Cytograph {
        let g = this.createUnderlyingGraph(profile);
        let result = new Cytograph(g);
        let inserted = new OMap<NodeId, GraphNode>();
        let sources = profile.sources.unwrapOr(new Sources([]));

        let visibleParents = new Set<NodeId>();
        for (let [nodeId, node] of profile.simpleNodes.entries()) {
            // Find out whether we display this node or the innermost region that hides it.
            const drawn = Cytograph.drawnNode(profile, selection, nodeId);
            if (drawn !== nodeId) {
                let set = result.nodeChildren.get(drawn);
                if (set.isSome())
                    set.unwrap().add(nodeId);
                else {
                    result.nodeChildren.set(drawn, new Set([nodeId]));
                }
            }

            let hasChildren = false;
            if (drawn !== nodeId) {
                if (inserted.has(drawn))
                    // Another child has inserted this region
                    continue;
                nodeId = drawn;
                hasChildren = true;
                node = profile.complexNodes.get(nodeId).unwrap();
                // Note: above we switched the nodeId/node that we are processing.
            }

            let parent = profile.parents.get(node.id);
            if (parent.isSome()) {
                // Every region around the drawn node is drawn too, expanded: cytoscape needs each
                // `parent` it is given to be a node on the graph, up to the outermost one.
                let ancestor = parent;
                while (ancestor.isSome()) {
                    visibleParents.add(ancestor.unwrap());
                    ancestor = profile.parents.get(ancestor.unwrap());
                }
            }
            let src = sources.toString(node.sourcePositions);
            let operation = node instanceof ComplexNode
                // node is complex only when drawn collapsed; show the tables and views hidden inside
                ? node.collapsedOperation()
                : node.operation;
            if (operation === CircuitProfile.Z1_TRACE_OUTPUT)
                // These nodes were modified in the profile.fixZ1Nodes() function.
                operation = CircuitProfile.Z1_TRACE;
            const leafCount = node instanceof ComplexNode ? node.leafCount : 0;
            // Never expanded: a region is only drawn here when it is collapsed, and the expanded ones
            // are added by the loop below.
            let graphNode = new GraphNode(nodeId, node.persistentId, operation, hasChildren, false, parent, src, leafCount);
            result.addNode(graphNode);
            inserted.set(nodeId, graphNode);
        }

        for (const nodeId of profile.complexNodes.keys()) {
            let complex = profile.complexNodes.get(nodeId).unwrap();
            let parent = profile.parents.get(nodeId);
            if (profile.isTop(nodeId) || // always create a node for the toplevel graph
                visibleParents.has(nodeId)) {
                let positions = complex.sourcePositions;
                let src = sources.toString(positions);
                let node = new GraphNode(nodeId, complex.persistentId, complex.operation, true, true, parent, src, complex.leafCount);
                result.addNode(node);
            }
        }

        // Keys are pairs (Simple node ID, target node ID), where target is simple or complex
        // Used to avoid inserting the same edge twice between two complex nodes
        // if the edge represents the same channel.
        let insertedEdges: Set<string> = new Set();
        for (const edge of profile.edges) {
            let source = edge.source;
            let target = edge.target;
            let originalEdge = result.createEdge(source, target, edge.back);

            if (profile.complexNodes.has(target))
                // Do not add edges to complex nodes.
                continue;

            // An edge to or from a node that is not drawn lands on the region drawn in its place.
            source = Cytograph.drawnNode(profile, selection, source);
            target = Cytograph.drawnNode(profile, selection, target);
            const targetCollapsed = target !== edge.target;

            let sourceNode = inserted.get(source).expect(`Node ${source} not found in visible map`);
            let targetNode = inserted.get(target).expect(`Node ${target} not found in visible map`);

            if (sourceNode === targetNode) {
                // Induced edges can be self-edges; do not add these
                continue;
            }

            // Detect whether an edge represents the same channel as a previous edge
            // (only suppressed if the edge goes to a complex node)
            if (targetCollapsed) {
                let pair = edge.source.toString() + "," + targetNode.id.toString();
                if (insertedEdges.has(pair)) {
                    continue;
                }
                insertedEdges.add(pair);
            }

            let realEdge = result.createEdge(sourceNode.id, targetNode.id, edge.back);
            result.edges.push(realEdge);
            result.mapEdge(originalEdge, realEdge);
        }
        return result;
    }

    /** Compute the difference between this and the other Cytographs */
    diff(other: Cytograph): GraphDiff {
        let nodes = ZSet.fromIterator(this.nodes);
        let otherNodes = ZSet.fromIterator(other.nodes);
        let nodeDiff = nodes.minus(otherNodes);

        let edges = ZSet.fromIterator(this.edges);
        let otherEdges = ZSet.fromIterator(other.edges);
        let edgeDiff = edges.minus(otherEdges);
        return new GraphDiff(nodeDiff, edgeDiff);
    }
}

/** Represents the diff between two graphs, as a pair of ZSets of nodes and edges. */
class GraphDiff {
    constructor(readonly nodes: ZSet<GraphNode>, readonly edges: ZSet<GraphEdge>) { }

    toString(): string {
        let result = this.nodes.toString();
        result += "\n" + this.edges.toString();
        return result;
    }
}

/** Handles updates and rendering for the displayed cytograph. */
export class CytographRendering {
    currentGraph: Cytograph | null;
    readonly cy: cytoscape.Core;
    readonly navigator: ViewNavigator;
    /**
     * If true do not remove the node information from the screen on mouse leave
     */
    stickyInformation: boolean;
    // Last node that triggered a recomputation of the layout
    lastNode: Option<NodeId>;
    // Current node that has tooltip displayed (for refreshing on metadata changes)
    private currentTooltipNode: NodeId | null = null;
    // True between `initiateLayout` and its matching `layoutComplete`. Used so `dispose()`
    // can fire a final `onRenderingChange(false)` if the layout was still in flight when the
    // visualizer is torn down — otherwise a consumer's progress bar would stick on screen.
    private renderingInFlight = false;

    constructor(
        graphContainer: HTMLElement,
        navigatorContainer: HTMLElement,
        private readonly callbacks: ProfilerCallbacks,
        readonly graph: Graph<NodeId>,
        readonly rootNodeId: NodeId,
        readonly selection: CircuitSelection,
        private metadataSelection: MetadataSelection,
        private message: (msg: string) => void,
        private clearMessage: () => void,
        private onTooltipContextChanged: () => void,
        private theme: DiagramTheme = 'light') {
        cytoscape.use(elk);
        cytoscape.use(dblclick);

        this.navigator = new ViewNavigator(navigatorContainer, this.theme);
        this.currentGraph = null;
        this.stickyInformation = false;
        // Start with an empty graph
        this.cy = cytoscape({
            container: graphContainer,
            elements: [],
        });
        // double-clicking on the navigator will adjust the graph to fit
        this.navigator.setOnDoubleClick(() => this.cy.fit());
        // a press or a drag on it moves the view to where it points
        this.navigator.setOnMoveTo((point) => this.panTo(point));
        installNodeShadows(this.cy);
        installNodeText(this.cy, () => this.theme);
        this.cy.style(buildGraphStyle(this.theme));
        this.lastNode = Option.none();
    }

    /** Metric chosen by the user to drive the color of the nodes. */
    getCurrentMetric(): string {
        return this.metadataSelection.metric;
    }

    /** Center the view on this node after the next layout completes. */
    centerOnNextLayout(node: Option<NodeId>) {
        this.lastNode = node;
    }

    /** Search a node by ID, return 'true' if found. */
    search(value: string): boolean {
        let el = this.cy.getElementById(value);
        // el may be an empty collection
        if (!el.nonempty()) {
            return false;
        }
        this.center(Option.some(value));
        this.markSelected(el.nodes());
        return true;
    }

    // Layout to use for the first graph rendering
    readonly initialLayout = {
        animate: false,
        nodeLayoutOptions: elkNodeLayoutOptions,
        fit: true,
        nodeDimensionsIncludeLabels: true,
        name: 'elk',
        elk: {
            'algorithm': 'layered',
            'elk.direction': 'DOWN',
            'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
            'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
            'elk.layered.nodePlacement.strategy': 'BRANDES_KOEPF'
        }
    };

    // Layout to use for subsequent renderings
    readonly layoutOptions = {
        animate: false,
        nodeLayoutOptions: elkNodeLayoutOptions,
        fit: false,
        nodeDimensionsIncludeLabels: true,
        name: 'elk',
        elk: {
            'algorithm': 'layered',
            'elk.direction': 'DOWN',
            'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
            // try to preserve the positions already computed
            'elk.layered.nodeplacement.strategy': 'INTERACTIVE'
        }
    };

    /** The graph has changed; adjust the display; this completes asynchronously */
    updateGraph(newGraph: Cytograph) {
        this.cy.startBatch();
        this.cy.container()!.style.visibility = "hidden";
        if (this.currentGraph === null) {
            // This is the first graph displayed.
            this.currentGraph = newGraph;
            this.cy.add(newGraph.getGraphElements(this.theme));
            this.cy.endBatch();
            return this.initiateLayout(this.initialLayout);
        } else {
            // Compute a diff between the previous and current graph.
            let graphDiff = newGraph.diff(this.currentGraph);
            this.currentGraph = newGraph;
            this.applyDiff(this.cy, graphDiff);
            this.cy.endBatch();
            return this.initiateLayout(this.layoutOptions);
        }
    }

    /** Center the visualization around the node with the specified id. */
    center(node: Option<NodeId>): void {
        if (!node.isSome()) {
            return;
        }

        const el = this.cy.getElementById(node.unwrap());
        let size = el.renderedHeight();
        let desiredSize = 15;
        // We determine the minimum size of found node by its height, because it is tied to font size
        if (size < desiredSize) {
            let zoom = this.cy.zoom();
            let targetZoom = zoom * desiredSize / size;
            this.cy.zoom({
                level: targetZoom,
                position: el.position()
            });
        }
        this.cy.center(el);
    }

    topNodes(profile: CircuitProfile, metric: string): Array<NodeAndMetric> {
        return this.currentGraph?.topNodes(profile, metric) || [];
    }

    /** Get a handle to the node in the rendering with the specified id. */
    getRenderedNode(node: NodeId): NodeSingular {
        return this.cy.getElementById(node) as NodeSingular;
    }

    static toMeasurement(m: PropertyValue, range: NumericRange): SerializedMeasurement {
        let percentile;
        let value = m.getNumericValue();
        if (value.isNone())
            percentile = 0;
        else if (!range.isEmpty() && !range.isPoint()) {
            percentile = range.percents(value.unwrap());
        } else {
            percentile = 0;
        }

        return new SerializedMeasurement(m, percentile);
    }

    /** Compute the attributes for all cytograph nodes based on the circuit profile and current selection. */
    computeAttributes(profile: CircuitProfile, selection: MetadataSelection,
        scales: DisplayScales) {
        let workers = selection.workersVisible.getSelectedElements(profile.getWorkerNames());
        // One column per visible worker. Aggregates such as min/max are not produced here:
        // consumers that want them compute them from the per-worker values themselves.
        let columnNames = workers.map(w => w.toString());
        for (const node of this.currentGraph!.nodes) {
            let profileNode = profile.getNode(node.getId()).unwrap();
            let data = new Map<string, Array<SerializedMeasurement>>();
            // Totals over the same workers the cells show, so they match what is displayed.
            let values = new Map<string, PropertyValue>();
            // Select just the visible metrics
            // Compute per-worker attributes
            for (let metric of profileNode.measurements.getMetrics()) {
                let range = scales.get(metric)?.range ?? NumericRange.empty();
                let metrics = profileNode.getMeasurements(metric);
                let selected = selection.workersVisible.getSelectedElements(metrics);
                let measurements: Array<SerializedMeasurement> = [];
                for (const m of selected) {
                    measurements.push(CytographRendering.toMeasurement(m, range));
                }
                data.set(metric, measurements);
                let total = profileNode.totalOf(metric, selected);
                if (total.isSome()) {
                    values.set(metric, total.unwrap());
                }
            }
            // The toplevel node stands for the whole circuit, so it holds the largest total of
            // every metric and has to be shaded against the metrics beside it instead.
            const overview = node.getId() === this.rootNodeId;
            const shares = overview ? categoryShares(values) : undefined;
            let totals = new Map<string, SerializedMeasurement>();
            for (const [metric, total] of values) {
                const share = overview
                    ? shares!.get(metric) ?? 0
                    : totalShare(total, scales.get(metric)?.maximum);
                totals.set(metric, new SerializedMeasurement(total, share));
            }
            // additional key-value per node attributes
            let kv = new Map();
            kv.set("id", node.id);
            if (node.persistentId.isSome()) {
                kv.set("persistentId", node.persistentId.unwrap());
            }
            kv.set("operation", node.getLabel());

            let parent = profile.parents.get(node.id);
            if (parent.isSome()) {
                const p = parent.unwrap();
                kv.set("parent", p);
            }
            let matrix = new MeasurementMatrix(
                columnNames, [...profileNode.measurements.getMetrics()], data, totals);
            let attributes = new Attributes(matrix, kv);
            let rendered = this.getRenderedNode(node.getId());
            rendered.data("expanded", node.expanded);
            rendered.data("has_children", node.hasChildren);
            // attach the attributes to the node
            rendered.data("attributes", attributes);
            if (node.getId() === this.rootNodeId) {
                rendered.data("invisible", true);
            }
        }
    }

    /** Compute the "importance" of each node given a selection, i.e., the way it's highlighted in the rendering. */
    computeImportance(profile: CircuitProfile, selection: MetadataSelection,
        scales: DisplayScales) {
        let range = scales.get(selection.metric)?.range;
        for (const node of this.currentGraph!.nodes) {
            if (node.expanded) { continue; }
            let profileNode = profile.getNode(node.getId()).unwrap();
            let percents = 0;
            if (range !== undefined && !range.isEmpty() && !range.isPoint()) {
                let m = profileNode.getMeasurements(selection.metric);
                let values = m.map(v => v.getNumericValue()).filter(v => v.isSome()).map(v => v.unwrap());
                let max = Math.max(...values, 0);
                percents = range.percents(max);
            }
            let rendered = this.getRenderedNode(node.getId());
            rendered.data("value", percents);
        }
    }

    /** The nodes whose readings set the scales: the ones actually drawn.  An expanded region is
     * drawn as the nodes inside it, so those set the scale and it does not; a collapsed one
     * stands for everything inside it.  The root is never drawn. */
    private drawnNodes(): Array<NodeId> {
        return this.currentGraph!.nodes
            .filter(node => !node.expanded && node.getId() !== this.rootNodeId)
            .map(node => node.getId());
    }

    /** The user has changed something in the way they want to visualize metadata; update the rendered graph. */
    updateMetadata(profile: CircuitProfile, selection: MetadataSelection) {
        if (this.currentGraph === null)
            return;
        this.metadataSelection = selection;
        // One pass over the drawn nodes feeds the node highlighting, the per-worker bars and the
        // totals, so all three describe the same population.
        const scales = profile.displayScales(this.drawnNodes(),
            values => selection.workersVisible.getSelectedElements(values));
        this.computeImportance(profile, selection, scales);
        this.computeAttributes(profile, selection, scales);
        this.cy.style().update();

        // Refresh the tooltip if there's a node currently displayed
        if (this.currentTooltipNode !== null) {
            const node = this.getRenderedNode(this.currentTooltipNode);
            if (node) {
                this.displayNodeAttributes(node);
            }
        }
    }

    /** Called when the graph has changed to trigger a new layout computation. */
    initiateLayout(options: any) {
        if (this.cy === null) {
            return;
        }
        // The layout runs asynchronously; the `true` dispatched here is paired with the
        // `false` dispatched from `layoutComplete()` on the `layoutstop` event. If `.run()`
        // throws synchronously (bad options, cytoscape internal error), we'd never reach
        // `layoutstop` and the consumer's progress bar would stick forever — so reset on the
        // throw before rethrowing.
        this.renderingInFlight = true;
        this.callbacks.onRenderingChange?.(true);
        this.message("Computing layout...");
        try {
            this.cy
                .layout(options)
                .run();
        } catch (e) {
            this.renderingInFlight = false;
            this.callbacks.onRenderingChange?.(false);
            throw e;
        }
    }

    /** Modify the rendered graph incrementally by applying a diff. */
    applyDiff(cy: cytoscape.Core, diff: GraphDiff) {
        // A parent node must be inserted before its children
        // so we need to compute dependencies between nodes.

        // Maps a node id of a parent to a list of children that are waiting for it
        // to be inserted.
        let childrenWaitingFor: Map<string, Array<GraphNode>> = new Map();
        let toInsert: Array<GraphNode> = new Array();
        // List of node ids for nodes in toInsert
        let inserted: Set<NodeId> = new Set();

        for (const [node, weight] of diff.nodes.entries()) {
            let id = node.getId();
            if (weight > 0) {
                if (node.getParent().isNone()) {
                    toInsert.push(node);
                    inserted.add(id);
                    if (childrenWaitingFor.has(id)) {
                        let waiting = childrenWaitingFor.get(id)!;
                        toInsert.push(...waiting);
                        childrenWaitingFor.delete(id);
                    }
                } else {
                    let parent = node.getParent().unwrap();
                    if (inserted.has(parent)) {
                        toInsert.push(node);
                    } else if (childrenWaitingFor.has(parent)) {
                        childrenWaitingFor.get(parent)!.push(node);
                    } else {
                        childrenWaitingFor.set(parent, new Array(node));
                    }
                }
            } else {
                cy.$id(id).remove();
            }
        }

        // If some parents we are waiting for are not in the current diff, they are
        // hopefully already in the graph; insert them now
        for (const a of childrenWaitingFor.values()) {
            toInsert.push(...a);
        }

        // Now add then to the graph in the right order
        for (const node of toInsert) {
            cy.add(node.getDefinition(this.theme));
        }

        for (const [edge, weight] of diff.edges.entries()) {
            let def = edge.getDefinition();
            if (weight > 0) {
                cy.add(def);
            } else {
                cy.$id(edge.getId()).remove();
            }
        }
    }

    keyup(event: KeyboardEvent) {
        if (event.key === "Escape") {
            this.setStickyNodeInformation(false);
            this.hideNodeInformation();
        }
    }

    setStickyNodeInformation(sticky: boolean) {
        this.stickyInformation = sticky;
    }

    setEvents(callbacks: {
        onNodeDoubleClick?: ((node: NodeId, type: 'group' | 'leaf') => void) | undefined
    }) {
        document.addEventListener('keyup', (e) => this.keyup(e));
        this.cy
            //.on('render', () => console.log("rendering"))
            //.on('layoutstart', () => console.log("start layout"))
            .on('layoutstop', () => this.layoutComplete())
            .on('mouseover', 'node', event => this.hoverNode(event))
            .on('mouseout', 'node', event => this.mouseOut(event))
            .on('zoom pan resize', () => this.updateNavigator(this.navigator))
            .on('click', 'node', (e) => this.reportOn(e.target.id()))
            .on('dblclick', 'node', (e) => {
                let node = e.target as NodeSingular;
                let id = e.target.id();

                if (node === null || !node.data("has_children")) {
                    // Leaf node - dispatch dedicated double click
                    callbacks.onNodeDoubleClick?.(id, 'leaf');
                    return
                }

                // Group node - toggle expand/collapse and dispatch dedicated double click
                this.hideNodeInformation();
                this.setStickyNodeInformation(false);
                this.lastNode = Option.some(id);
                callbacks.onNodeDoubleClick?.(id, 'group');
            });
    }

    /** Report on a node: what a click on it comes down to. */
    private reportOn(id: NodeId) {
        // Whatever was reported before goes first, so nothing of it survives into this node's report.
        this.hideNodeInformation();
        // Fires before the attributes, so a consumer can tell a click from a programmatic refresh.
        this.callbacks.onNodeClick?.(id);
        // A click is deliberate, so what it reports stays on screen, and an expanded region reports as
        // readily as an operator. A hover does neither, see `hoverNode`.
        this.setStickyNodeInformation(true);
        this.displayNodeAttributes(this.getRenderedNode(id));
    }

    layoutComplete() {
        this.clearMessage();
        this.renderingInFlight = false;
        this.callbacks.onRenderingChange?.(false);
        this.cy.container()!.style.visibility = "visible";
        this.navigator.showGraph(this.cy);
        this.updateNavigator(this.navigator);
        if (this.lastNode.isSome()) {
            this.center(this.lastNode);
            this.lastNode = Option.none();
        }
        // Set minimum/maximum zoom levels
        // Do not allow to zoom in more than 1.5; this should be enough to make any node visible
        this.cy.maxZoom(1.5);
        const rect = this.cy.container()?.getBoundingClientRect();
        if (rect !== undefined) {
            const bb = this.cy.elements().boundingBox();
            let maxRatio = Math.min(rect.height / bb.h, rect.width / bb.w);
            // Do not allow zoom out more than required to fit the entire graph
            this.cy.minZoom(maxRatio);
        }
    }

    // The user has panned/zoomed => tell the navigator about it.
    updateNavigator(navigator: ViewNavigator) {
        // Cytoscape's resize observer is debounced, so a `resize` fired on the way out arrives after
        // the instance has been destroyed - and a destroyed instance has no renderer left to answer
        // `extent()`.
        if (this.cy.destroyed()) {
            return;
        }
        navigator.showView(this.cy.extent());
    }

    /** Pan so that this model point is the center of the view. */
    private panTo(point: { x: number, y: number }) {
        const zoom = this.cy.zoom();
        this.cy.pan({
            x: this.cy.width() / 2 - point.x * zoom,
            y: this.cy.height() / 2 - point.y * zoom
        });
    }

    getActualEdgeId(e: Edge<NodeId>): string {
        let edgeId = e.id;
        if (this.currentGraph!.edgeMap.has(edgeId)) {
            return this.currentGraph?.edgeMap.get(edgeId)!;
        }
        return edgeId;
    }

    // Compute the edges reachable to/from a node in the underlying graph
    // Note that the node can be a parent, in this case we compute the
    // edges reachable from ALL its children.
    reachableFrom(id: NodeId, forward: boolean): EdgeCollection {
        // If a node is complex, get the list of children
        let nodes = this.currentGraph!.getSimpleNodes(id);
        let result = this.cy!.collection();
        for (const node of nodes) {
            if (forward) {
                let reachable = this.graph.reachableFrom(node, e => !e.back);
                for (let e of reachable) {
                    let edgeId = this.getActualEdgeId(e);
                    let edge = this.cy!.getElementById(edgeId);
                    if (edge !== undefined)
                        result = result.union(edge);
                }
            } else {
                let reverseReachable = this.graph.canReach(node, e => !e.back);
                for (let e of reverseReachable) {
                    let edgeId = this.getActualEdgeId(e);
                    let edge = this.cy!.getElementById(edgeId);
                    if (edge !== undefined)
                        result = result.union(edge);
                }
            }
        }

        return result;
    }

    /** A callback that reports metrics of the node the pointer moved onto: its attributes, the edges that reach it,
     * and its source position. `mouseOut` reports when the pointer leaves.
     *
     *  Two cases when a hover is ignored: an expanded node, and anything if a user selected a node with a click. */
    private hoverNode(event: EventObject) {
        const node: NodeSingular = event.target;
        if (node.isParent()) {
            return;
        }
        if (this.stickyInformation) {
            if (!this.reportIsMarked()) {
                this.traceSelection(node);
            }
            return;
        }
        this.displayNodeAttributes(node);
    }

    /** Whether the node the metrics report is about is the node the diagram marks with the glow and
     * traces with the colored edges. When it is not, the mark is the pointer's to move: neither an
     * expanded region nor the root node get marked, and neither has a report whose node
     * a graph update has removed.
     *
     * Asked of the mark itself, so the two can never disagree about who holds it. */
    private reportIsMarked(): boolean {
        if (this.currentTooltipNode === null) {
            return false;
        }
        return this.getRenderedNode(this.currentTooltipNode).hasClass(SELECTED_NODE_CLASS);
    }

    /** Mark `node` as the one the diagram reports on, whether reached by click, hover or search. At most
     *  one node is marked, and `nodeShadow.ts` paints the mark as an accent glow in place of the node's
     *  ambient shadow. */
    markSelected(node: NodeSingular | NodeCollection | null) {
        this.cy.nodes(`.${SELECTED_NODE_CLASS}`).removeClass(SELECTED_NODE_CLASS);
        node?.addClass(SELECTED_NODE_CLASS);
    }

    /** Mark the node whose metrics are on display and color the edges reaching it.
     * An expanded region and the root node can not be marked. */
    private traceSelection(node: NodeSingular) {
        this.clearTrace();
        if (node.isParent() || node.id() === this.rootNodeId) {
            return;
        }
        this.markSelected(node);
        this.reachableFrom(node.id(), true).addClass('highlight-forward');
        this.reachableFrom(node.id(), false).addClass('highlight-backward');
    }

    /** Take the mark and the edge coloring off the diagram, leaving what is reported where it is. */
    private clearTrace() {
        this.markSelected(null);
        this.cy.edges().removeClass('highlight-forward highlight-backward');
    }

    displayNodeAttributes(node: NodeSingular) {
        const nodeId = node.id();
        const attributes: Attributes = node.data().attributes;
        const sources = node.data("sources")
        if (this.cy === null) {
            return;
        }

        // Track the current tooltip node for refreshing on metadata changes
        this.currentTooltipNode = nodeId;
        this.onTooltipContextChanged()
        this.traceSelection(node);

        // Build structured tooltip data
        let visible = false;

        const tooltipData: NodeAttributes = {
            nodeId,
            title: "",
            columns: [],
            rows: [],
            attributes: new Map()
        };

        // Add matrix attributes (per-worker metrics)
        if (attributes.matrix.attributeCount() > 0 && attributes.matrix.getColumnCount() > 0) {
            visible = true;

            // Set column headers (worker names)
            const colCount = attributes.matrix.getColumnCount();
            for (let i = 0; i < colCount; i++) {
                tooltipData.columns.push(attributes.matrix.columnNames[i]!);
            }

            // Add rows (metrics)
            const MAX_CELL_COUNT = 40;
            let matrix = attributes.matrix.getAttributes();
            let keys = attributes.matrix.metricOrder;

            for (const key of keys) {
                let values = matrix.get(key)!;
                const cells: TooltipCell[] = [];

                // Limit to MAX_CELL_COUNT cells per metric
                for (let i = 0; i < Math.min(values.length, MAX_CELL_COUNT); i++) {
                    const value = values[i];
                    if (value) {
                        cells.push({
                            value: value.value,
                            percentile: value.percentile
                        });
                    }
                }

                const total = attributes.matrix.totals.get(key);
                tooltipData.rows.push({
                    metric: key,
                    isCurrentMetric: key === this.getCurrentMetric(),
                    cells,
                    total: total && { value: total.value, percentile: total.percentile }
                });
            }
        }

        // Add source position information
        if (sources.length > 0) {
            visible = true;
            tooltipData.sources = sources;
        }

        // Add additional key-value attributes
        if (attributes.kv.size !== 0) {
            visible = true;
            for (const [key, value] of attributes.kv.entries()) {
                if (key == "id") {
                    tooltipData.title = value + tooltipData.title;
                } else if (key == "operation") {
                    tooltipData.title = tooltipData.title + " " + value;
                } else {
                    tooltipData.attributes.set(key, value);
                }
            }
        }

        if (!visible)
            return;

        // Send tooltip data via callback
        this.callbacks.displayNodeAttributes(Option.some(tooltipData), this.stickyInformation);
    }

    /** The other half of a hover (see `hoverNode`): a report the pointer brought with it goes when the
     *  pointer does. */
    mouseOut(_event: EventObject) {
        if (!this.stickyInformation) {
            this.hideNodeInformation();
            return;
        }
        // What the pointer marked and traced goes with the pointer; the report itself was asked for,
        // and stays.
        if (!this.reportIsMarked()) {
            this.clearTrace();
        }
    }

    hideNodeInformation() {
        this.currentTooltipNode = null;
        this.clearTrace();
        this.onTooltipContextChanged()
        this.callbacks.displayNodeAttributes(Option.none(), false);
    }

    /**
     * Clean up resources when the rendering is no longer needed
     */
    dispose(): void {
        // Destroying cytoscape mid-layout cancels the pending `layoutstop` event, so a
        // consumer driving a progress bar from `onRenderingChange` would never see the
        // matching `false`. Emit it explicitly here for the in-flight case.
        if (this.renderingInFlight) {
            this.renderingInFlight = false;
            this.callbacks.onRenderingChange?.(false);
        }

        // Destroy the Cytoscape instance
        if (this.cy) {
            this.cy.destroy();
        }

        // Hide tooltip
        this.callbacks.displayNodeAttributes(Option.none(), false);

        // Clear references
        this.currentGraph = null;
    }
}