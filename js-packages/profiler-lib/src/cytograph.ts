// A cytograph is a graph representation suitable for the cytoscape graph layout and rendering tool

import cytoscape, { type EdgeCollection, type EdgeDefinition, type ElementsDefinition, type EventObject, type NodeDefinition, type NodeSingular, type StylesheetJson } from 'cytoscape';
import dblclick from 'cytoscape-dblclick';
import { assert, Graph, OMap, Option, type EncodableAsString, NumericRange, Edge } from './util.js';
import { CircuitProfile, type NodeId } from './profile.js';
import { CircuitSelection } from './selection.js';
import elk from 'cytoscape-elk';
import { Sources } from './dataflow.js';
import { Point, Size } from "./planar.js";
import { ViewNavigator } from './navigator.js';
import { ZSet } from "./zset.js";
import { MetadataSelection } from './metadataSelection.js';
import { type DisplayedAttributes, type TooltipCell, type ProfilerCallbacks } from './profiler.js';

/** A measurement represented as a string, but also with a normalized value between 0 and 100. */
class SerializedMeasurement {
    constructor(readonly value: string, readonly percentile: number) { }

    toString(): string {
        return this.value;
    }
}

/** A matrix of measurements */
class MeasurementMatrix {
    constructor(
        // There should be one column name for each value in the attributes array
        readonly columnNames: Array<string>,
        // Keys are measurement names, arrays contain one element per column name.
        readonly attributes: Map<string, Array<SerializedMeasurement>>) {
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
        // True if the node has source position information
        readonly hasSourcePosition: boolean) {
    }

    asString(): string {
        return this.id;
    }

    getParent() {
        return this.parent;
    }

    /** Returns a data structure understood by cytoscape for a node. */
    getDefinition(): NodeDefinition {
        // Add a small SQL prefix for nodes with source positions
        const prefix = this.hasSourcePosition ? "◆ " : "";
        const label = prefix + this.id + " " + this.label;

        let result = {
            "data": {
                "id": this.id,
                "label": label,
                "sources": this.sources,
                "has_source": this.hasSourcePosition
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
    getGraphElements(): ElementsDefinition {
        return {
            "nodes": this.nodes.map(n => n.getDefinition()),
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

    // Create a Cytograph from a CircuitProfile filtered by the specified selection.
    static fromProfile(profile: CircuitProfile, selection: CircuitSelection): Cytograph {
        let g = this.createUnderlyingGraph(profile);
        let result = new Cytograph(g);
        let inserted = new OMap<NodeId, GraphNode>();
        let sources = profile.sources.unwrapOr(new Sources([]));

        let visibleParents = new Set<NodeId>();
        for (let [nodeId, node] of profile.simpleNodes.entries()) {
            // Find out whether we display this node or only its parent.
            // If the parent is expanded, we display this node.
            let topParent = profile.getTopParent(nodeId);
            const expand = selection.regionsExpanded.contains(topParent);
            if (nodeId !== topParent && !expand) {
                let set = result.nodeChildren.get(topParent);
                if (set.isSome())
                    set.unwrap().add(nodeId);
                else {
                    result.nodeChildren.set(topParent, new Set(nodeId));
                }
            }

            let hasChildren = false;
            if (!expand && nodeId !== topParent) {
                if (inserted.has(topParent))
                    // Another child has inserted this parent
                    continue;
                nodeId = topParent;
                hasChildren = true;
                node = profile.complexNodes.get(nodeId).unwrap();
                // Note: above we switched the nodeId/node that we are processing.
            }

            let parent = profile.parents.get(node.id);
            if (parent.isSome()) {
                const p = parent.unwrap();
                visibleParents.add(p);
            }
            let src = sources.toString(node.sourcePositions);
            let hasSource = src.length > 0;
            node.getMeasurements("operation");

            let operation = node.operation;
            if (operation === CircuitProfile.Z1_TRACE_OUTPUT)
                // These nodes were modified in the profile.fixZ1Nodes() function.
                operation = CircuitProfile.Z1_TRACE;
            let visibleNode = new GraphNode(nodeId, node.persistentId, operation, hasChildren, expand && hasChildren, parent, src, hasSource);
            result.addNode(visibleNode);
            inserted.set(nodeId, visibleNode);
        }

        for (const nodeId of profile.complexNodes.keys()) {
            let complex = profile.complexNodes.get(nodeId).unwrap();
            let parent = profile.parents.get(nodeId);
            if (!profile.isTop(nodeId) && visibleParents.has(nodeId)) {
                let positions = complex.sourcePositions;
                let src = sources.toString(positions);
                let hasSource = src.length > 0;
                let node = new GraphNode(nodeId, complex.persistentId, "region", true, true, parent, src, hasSource);
                result.addNode(node);
            }
        }

        for (const edge of profile.edges) {
            let source = edge.source;
            let target = edge.target;
            let originalEdge = result.createEdge(source, target, edge.back);

            let sourceParent = profile.getTopParent(source);
            let targetParent = profile.getTopParent(target);
            let expandSource = selection.regionsExpanded.contains(sourceParent);
            let expandTarget = selection.regionsExpanded.contains(targetParent);
            if (profile.complexNodes.has(target))
                // Do not add edges to complex nodes.
                continue;

            if (!expandSource)
                source = sourceParent;
            if (!expandTarget)
                target = targetParent;

            let sourceNode = inserted.get(source).expect(`Node ${source} not found in visible map`);
            let targetNode = inserted.get(target).expect(`Node ${target} not found in visible map`);

            if (sourceNode !== targetNode) {
                // Induced edges can be self-edges; do not add these
                let realEdge = result.createEdge(sourceNode.id, targetNode.id, edge.back);
                result.edges.push(realEdge);
                result.mapEdge(originalEdge, realEdge);
            }
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
    // If true do not remove the node information from the screen on mouse leave
    stickyInformation: boolean;
    // Last node that triggered a recomputation of the layout
    lastNode: Option<NodeId>;

    readonly graph_style: StylesheetJson = [
        {
            // How to display nodes
            selector: 'node',
            css: {
                'shape': 'rectangle',
                'content': 'data(label)',
                'text-valign': 'center',
                'text-halign': 'center',
                "font-size": "12px",
                'height': '14px',
                'line-color': "black",
                'border-color': 'black',
                'border-width': '1px',
                'border-style': 'solid',
                'padding': '2px',
                'width': 'label',
            }
        },
        {
            // SQL source indicator - thicker blue border for nodes with source positions available
            // TODO: To improve the design we can use https://github.com/kaluginserg/cytoscape-node-html-label
            // to implement the badge that indicates that a node has the source position available
            selector: 'node[?has_source]',
            css: {
                'border-width': '3px',
                'border-color': '#4a90e2',
                'border-style': 'solid',
            }
        },
        {
            // How to color nodes based on the 'value' attribute
            selector: 'node[value]',
            css: {
                'background-color': "mapData(value, 0, 100, white, red)",
            }
        },
        {
            // How to display "hidden" nodes; currently unused
            selector: 'node[hidden]',
            style: {
                'border-style': 'dotted',
                'border-color': 'black',
                'border-width': '1px',
                'width': 'label',
            }
        },
        {
            // How to display nodes which have children
            selector: 'node[?has_children]',
            style: {
                'shape': 'round-rectangle',
            }
        },
        {
            // How to color nodes that have children.
            selector: 'node[depth]:parent',
            style: {
                'label': '',
                'text-opacity': 0,
                'text-events': 'no',
                'background-color': 'mapData(depth, 0, 5, #f5f5f5, #d0d0d0)',
            }
        },
        {
            // How to display nodes which have children
            selector: ':parent',
            css: {
                'text-valign': 'top',
                'text-halign': 'center',
                'shape': 'round-rectangle',
                'corner-radius': "10",
                'padding': "10"
            }
        },
        {
            // How to style edges
            selector: 'edge',
            css: {
                'curve-style': 'bezier',
                'target-arrow-shape': 'triangle',
                'line-color': 'black',
                'target-arrow-color': 'black',
                'width': 2
            }
        },
        {
            // How to display forward edges that are highlighted
            selector: 'edge.highlight-backward',
            style: {
                'line-color': 'blue',
                'target-arrow-color': 'blue',
                'width': 3
            }
        },
        {
            // How to display backward edges that are highlighted
            selector: 'edge.highlight-forward',
            style: {
                'line-color': 'red',
                'target-arrow-color': 'red',
                'width': 3
            }
        },
    ];

    constructor(
        graphContainer: HTMLElement,
        navigatorContainer: HTMLElement,
        private readonly callbacks: ProfilerCallbacks,
        readonly graph: Graph<NodeId>,
        readonly selection: CircuitSelection,
        private metadataSelection: MetadataSelection,
        private message: (msg: string) => void,
        private clearMessage: () => void) {
        cytoscape.use(elk);
        cytoscape.use(dblclick);

        this.navigator = new ViewNavigator(navigatorContainer);
        this.currentGraph = null;
        this.stickyInformation = false;
        // Start with an empty graph
        this.cy = cytoscape({
            container: graphContainer,
            elements: [],
        });
        // double-clicking on the navigator will adjust the graph to fit
        this.navigator.setOnDoubleClick(() => this.cy.fit());
        this.cy.style(this.graph_style);
        this.lastNode = Option.none();
    }

    /** Metric chosen by the user to drive the color of the nodes. */
    getCurrentMetric(): string {
        return this.metadataSelection.metric;
    }

    search(value: string) {
        let el = this.cy.getElementById(value);
        if (el === null) {
            return;
        }
        this.center(Option.some(value));
    }

    // Layout to use for the first graph rendering
    readonly initialLayout = {
        animate: false,
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
            this.cy.add(newGraph.getGraphElements());
            this.cy.endBatch();
            return this.initiateLayout(this.initialLayout);
        } else {
            // Compute a diff between the previous and current graph.
            let graphDiff = newGraph.diff(this.currentGraph);
            this.currentGraph = newGraph;
            this.applyDiff(this.cy, graphDiff);
            this.cy.nodes().forEach(n => {
                const depth = n.ancestors().filter(n => n.isNode()).length;
                n.data('depth', depth);
            });
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

    /** Get a handle to the node in the rendering with the specified id. */
    getRenderedNode(node: NodeId): NodeSingular {
        return this.cy.getElementById(node) as NodeSingular;
    }

    static percentile(m: Option<number>, range: NumericRange): number {
        if (m.isNone())
            return 0;
        if (!range.isEmpty() && !range.isPoint()) {
            return range.percents(m.unwrap());
        }
        return 0;
    }

    /** Compute the attributes for all cytograph nodes based on the circuit profile and current selection. */
    computeAttributes(profile: CircuitProfile, selection: MetadataSelection) {
        for (const node of this.currentGraph!.nodes) {
            let profileNode = profile.getNode(node.getId()).unwrap();
            let workers = selection.workersVisible.getSelectedElements(profile.getWorkerNames());
            let columnNames = workers.map(w => w.toString());
            let data = new Map<string, Array<SerializedMeasurement>>();
            // Select just the visible metrics
            // Compute per-worker attributes
            for (let metric of profileNode.measurements.getMetrics()) {
                let range = profile.propertyRange(metric);
                let metrics = profileNode.getMeasurements(metric);
                let selected = selection.workersVisible.getSelectedElements(metrics);
                data.set(metric, selected.map(
                    m => new SerializedMeasurement(
                        m.toString(),
                        CytographRendering.percentile(m.getNumericValue(), range))));
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
            let matrix = new MeasurementMatrix(columnNames, data);
            let attributes = new Attributes(matrix, kv);
            let rendered = this.getRenderedNode(node.getId());
            rendered.data("expanded", node.expanded);
            rendered.data("has_children", node.hasChildren);
            // attach the attributes to the node
            rendered.data("attributes", attributes);
        }
    }

    /** Compute the "importance" of each node given a selection, i.e., the way it's highlighted in the rendering. */
    computeImportance(profile: CircuitProfile, selection: MetadataSelection) {
        let rangeO = profile.dataRange.get(selection.metric);
        for (const node of this.currentGraph!.nodes) {
            if (node.expanded) { continue; }
            let profileNode = profile.getNode(node.getId()).unwrap();
            let percents = 0;
            if (rangeO.isSome()) {
                let range = rangeO.unwrap();
                if (!range.isEmpty() && !range.isPoint()) {
                    let m = profileNode.getMeasurements(selection.metric);
                    m = selection.workersVisible.getSelectedElements(m);
                    let values = m.map(v => v.getNumericValue()).filter(v => v.isSome()).map(v => v.unwrap());
                    let max = Math.max(...values, 0);
                    percents = range.percents(max);
                }
            }
            let rendered = this.getRenderedNode(node.getId());
            rendered.data("value", percents);
        }
    }

    /** The user has changed something in the way they want to visualize metadata; update the rendered graph. */
    updateMetadata(profile: CircuitProfile, selection: MetadataSelection) {
        if (this.currentGraph === null)
            return;
        this.metadataSelection = selection;
        this.computeImportance(profile, selection);
        this.computeAttributes(profile, selection);
        this.cy.style().update();
    }

    /** Called when the graph has changed to trigger a new layout computation. */
    initiateLayout(options: any) {
        if (this.cy === null) {
            return;
        }
        // This runs asynchronously
        this.message("Computing layout...");
        this.cy
            .layout(options)
            .run();
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
            cy.add(node.getDefinition());
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
        onParentNodeDoubleClick?: ((node: NodeId) => void) | undefined,
        onLeafNodeDoubleClick?: ((node: NodeId) => void) | undefined
    }) {
        document.addEventListener('keyup', (e) => this.keyup(e));
        this.cy
            //.on('render', () => console.log("rendering"))
            //.on('layoutstart', () => console.log("start layout"))
            .on('layoutstop', () => this.layoutComplete())
            .on('mouseover', 'node', event => this.displayNodeAttributes(event))
            .on('mouseout', 'node', event => this.mouseOut(event))
            .on('zoom pan resize', () => this.updateNavigator(this.navigator))
            .on('click', 'node', (e) => {
                // Hide previous node if any
                this.setStickyNodeInformation(false);
                this.hideNodeInformation();
                // Display current node
                this.displayNodeAttributes(e);
                // Keep the information after mouse out
                this.setStickyNodeInformation(true);
            })
            .on('dblclick', 'node', (e) => {
                let node = e.target as NodeSingular;
                let id = e.target.id();

                if (node !== null && node.data("has_children")) {
                    // Parent node - toggle expand/collapse
                    this.hideNodeInformation();
                    this.setStickyNodeInformation(false);
                    this.lastNode = Option.some(id);
                    callbacks.onParentNodeDoubleClick?.(id);
                } else if (callbacks.onLeafNodeDoubleClick) {
                    // Leaf node - navigate to source position
                    callbacks.onLeafNodeDoubleClick(id);
                }
            });
    }

    layoutComplete() {
        // console.log("layout complete");
        this.clearMessage();
        this.cy.container()!.style.visibility = "visible";
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
        if (this.cy === null) {
            return;
        }
        const container = this.cy.container();
        if (container === null) {
            return;
        }

        let rect = container.getBoundingClientRect();
        const zoom = this.cy.zoom();
        const pan = this.cy.pan();
        const bb = this.cy.elements().boundingBox();
        navigator.setViewParameters(
            new Size(rect.width, rect.height),
            new Point(pan.x, pan.y),
            new Size(bb.w * zoom, bb.h * zoom));
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

    // Called when someones hovers over a node.
    // If the previous display is sticky, do nothing.
    // Currently it displays
    // (1) the attributes of the node,
    // (2) it highlights the edges reaching the node,
    // (3) it displays the source position of the node.
    displayNodeAttributes(event: EventObject) {
        if (this.cy === null || this.stickyInformation) {
            return;
        }

        let node: NodeSingular = event.target;
        if (node.data("expanded") === true)
            return;

        // highlight edges
        let reachable = this.reachableFrom(node.id(), true);
        reachable.addClass('highlight-forward');
        reachable = this.reachableFrom(node.id(), false);
        reachable.addClass('highlight-backward');

        // Build structured tooltip data
        let attributes: Attributes = node.data().attributes;
        let visible = false;

        const tooltipData: DisplayedAttributes = {
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
                tooltipData.columns.push(attributes.matrix.columnNames[i] || "");
            }

            // Add rows (metrics)
            const MAX_CELL_COUNT = 40;
            let matrix = attributes.matrix.getAttributes();
            let keys = [...matrix.keys()];
            keys.sort();

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

                tooltipData.rows.push({
                    metric: key,
                    isCurrentMetric: key === this.getCurrentMetric(),
                    cells
                });
            }
        }

        // Add source position information
        let sources = node.data("sources");
        if (sources.length > 0) {
            visible = true;
            tooltipData.sources = sources;
        }

        // Add additional key-value attributes
        if (attributes.kv.size !== 0) {
            visible = true;
            for (const [key, value] of attributes.kv.entries()) {
                tooltipData.attributes.set(key, value);
            }
        }

        if (!visible)
            return;

        // Send tooltip data via callback
        this.callbacks.displayNodeAttributes(Option.some(tooltipData), true);
    }

    // hide the information shown when hovering
    mouseOut(_event: EventObject) {
        if (!this.stickyInformation) {
            this.hideNodeInformation();
        }
    }

    hideNodeInformation() {
        this.callbacks.displayNodeAttributes(Option.none(), false);
        let reachable = this.cy.edges();
        reachable.removeClass('highlight-forward');
        reachable.removeClass('highlight-backward');
    }

    /**
     * Clean up resources when the rendering is no longer needed
     */
    dispose(): void {
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