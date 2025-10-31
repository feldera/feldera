import cytoscape, { type EdgeCollection, type EdgeDefinition, type ElementsDefinition, type EventObject, type NodeDefinition, type NodeSingular, type StylesheetJson } from 'cytoscape';
import dblclick from 'cytoscape-dblclick';
import { Graph, NumericRange, OMap, Option } from './util.js';
import { Globals } from './globals.js';
import { CircuitProfile, SimpleNode, type NodeId } from './profile.js';
import { CircuitSelection } from './selection.js';
import elk from 'cytoscape-elk';
import { Sources } from './dataflow.js';
import { Point, Size } from "./planar.js";
import { ViewNavigator } from './navigator.js';
import { ZSet } from "./zset.js";

/** Cytoscape attributes to be displayed for a node on hover. */
class Attributes {
    constructor(
        // There should be one column name for each value in the attributes array
        readonly columnNames: Array<string>,
        // Array attributes: one per column
        private readonly attributes: Map<string, Array<string>>,
        // Key-value attributes
        readonly kv: Map<string, string>,
        readonly sources: string
    ) { };

    attributeCount(): number {
        return this.attributes.size;
    }

    getAttributes(): Map<string, Array<string>> {
        return this.attributes;
    }

    getColumnCount(): number {
        return this.columnNames.length;
    }

    getSources(): string {
        return this.sources;
    }
}

interface GraphNode {
    getId(): NodeId;
    getDefinition(): NodeDefinition;
    getLabel(): string;
}

/** Cytoscape graph node that corresponds to a visible node in the circuit profile. */
class VisibleNode implements GraphNode {
    constructor(
        readonly id: NodeId,
        readonly label: string,
        // Used to determine color; between 0 and 100.
        readonly value: number = 0,
        readonly parent: Option<string> = Option.none(),
        // Attributes to display on hover.
        readonly attributes: Attributes = new Attributes([], new Map(), new Map(), "")
    ) { }

    static normalizeId(label: string): string {
        // Remove any leading "n" characters.
        return label.replace(/^n+/g, '');
    }

    getDefinition(): NodeDefinition {
        let result = { "data": { "id": this.id, "value": this.value, "label": VisibleNode.normalizeId(this.id) + " " + this.label } };
        if (this.parent.isSome()) {
            (result["data"] as any)["parent"] = this.parent.unwrap();
        }
        (result["data"] as any)["attributes"] = this.attributes;
        return result;
    }

    getLabel(): string {
        return this.label;
    }

    getId(): NodeId {
        return this.id;
    }
}

/** A Cytoscape graph node that represents multiple hidden profile nodes. */
class HiddenNode implements GraphNode {
    assigned: Set<NodeId> = new Set();
    parent: Option<string> = Option.none();

    constructor(
        readonly id: NodeId
    ) { }

    getId(): NodeId {
        return this.id;
    }

    addNode(nodeId: NodeId): HiddenNode {
        this.assigned.add(nodeId);
        return this;
    }

    setParent(parent: NodeId): void {
        this.parent = Option.some(parent);
    }

    getDefinition(): NodeDefinition {
        let result = { "data": { "id": this.id, "value": 0, "label": this.assigned.size.toString() + " nodes" } };
        let attributes = new Attributes(
            ["hidden"],
            new Map([["hidden nodes", Array.from(this.assigned).map(n => VisibleNode.normalizeId(n.toString()))]]),
            new Map(),
            ""
        );
        if (this.parent.isSome()) {
            (result["data"] as any)["parent"] = this.parent.unwrap();
        }
        (result["data"] as any)["attributes"] = attributes;
        (result["data"] as any)["hidden"] = true;
        return result;
    }

    getLabel(): string {
        return this.assigned.size.toString();
    }
}

/** Cytoscape graph edge. */
class GraphEdge {
    constructor(readonly source: string, readonly target: string, readonly backedge: boolean) { }

    getDefinition(): EdgeDefinition {
        let def: EdgeDefinition = { "data": { "source": this.source, "target": this.target } };
        if (this.backedge)
            def["classes"] = "back";
        return def;
    }
}

/** A directed graph which can be displayed using the Cytoscape rendering library. */
export class Cytograph {
    readonly nodes: Array<GraphNode>;
    readonly edges: Array<GraphEdge>;

    constructor(readonly metric: string, readonly graph: Graph<NodeId>, readonly propertyRange: OMap<string, NumericRange>) {
        this.nodes = [];
        this.edges = [];
    }

    addNode(node: GraphNode) {
        this.nodes.push(node);
    }

    addEdge(source: string, target: string, back: boolean = false) {
        let edge = new GraphEdge(source, target, back);
        if (!this.edges.some(e => e === edge))
            this.edges.push(edge);
    }

    getGraphElements(): ElementsDefinition {
        return {
            "nodes": this.nodes.map(n => n.getDefinition()),
            "edges": this.edges.map(e => e.getDefinition())
        }
    }

    toString(): string {
        return JSON.stringify(this.nodes) + "\n" + JSON.stringify(this.edges);
    }

    // Test example graph.
    static createTestExample(): Cytograph {
        let graph = new Cytograph("latency", new Graph<NodeId>(), new OMap<string, NumericRange>());
        const workers = ["0", "1"];
        graph.addNode(new VisibleNode("0", "filter", 100, Option.none(),
            new Attributes(workers, new Map([["latency", ["10ms", "20ms"]]]), new Map(), "")));
        graph.addNode(new VisibleNode("1", "map", 10));
        graph.addNode(new HiddenNode("2").addNode("join").addNode("hidden"));
        graph.addNode(new VisibleNode("3", "sink", 20));
        graph.addEdge("0", "1");
        graph.addEdge("1", "2");
        graph.addEdge("2", "3");
        graph.addEdge("0", "2");
        graph.addEdge("1", "2");
        return graph;
    }

    static createAttributes(node: SimpleNode, selection: CircuitSelection, profile: CircuitProfile): Attributes {
        let workers = selection.workersVisible.getSelectedElements(profile.getWorkerNames());
        let columnNames = workers.map(w => w.toString());
        let data = new Map<string, Array<string>>();
        // Select just the visible metrics
        let persistentId = Option.none<string>();
        for (let metric of node.measurements.getMetrics()) {
            let metrics = node.getMeasurements(metric);
            let selected = selection.workersVisible.getSelectedElements(metrics);
            data.set(metric, selected.map(m => m.toString()));
            if (metric === CircuitProfile.PERSISTENT_ID && metrics.length > 0)
                // Set the persistent ID for this node.
                persistentId = Option.some(metrics[0]!.getStringValue());
        }
        let src = profile.sources.unwrapOr(new Sources([])).toString(node.sourcePositions);
        let kv = new Map();
        kv.set("id", node.id);
        if (persistentId.isSome()) {
            kv.set("persistentId", persistentId.unwrap());
        }
        let operation = node.operation;
        if (operation === CircuitProfile.Z1_TRACE_OUTPUT)
            // These nodes were fixed in the profile fixZ1Nodes function.
            operation = CircuitProfile.Z1_TRACE;
        let parent = profile.parents.get(node.id);
        if (parent.isSome()) {
            const p = parent.unwrap();
            kv.set("parent", p);
        }
        kv.set("operation", operation);
        return new Attributes(columnNames, data, kv, src);
    }

    static createUnderlyingGraph(profile: CircuitProfile, selection: CircuitSelection): Graph<NodeId> {
        // Build the underlying graph based only on simple nodes
        let g: Graph<NodeId> = new Graph();
        for (const nodeId of profile.simpleNodes.keys()) {
            g.addNode(nodeId);
        }
        for (const edge of profile.edges) {
            let source = edge.source;
            let sourceHidden = !selection.nodesVisible.contains(source);
            let target = edge.target;
            let targetHidden = !selection.nodesVisible.contains(target);
            let weight = (sourceHidden && targetHidden) ? 0 : 1;
            if (profile.complexNodes.has(target))
                // Do not add edges to complex nodes.
                continue;
            g.addEdge(edge.source, edge.target, weight, edge.back);
        }
        return g;
    }

    // Create a Cytograph from a CircuitProfile filtered by the specified selection.
    static fromProfile(profile: CircuitProfile, selection: CircuitSelection): Cytograph {
        let g = this.createUnderlyingGraph(profile, selection);
        let depth = g.dfs();

        let graph = new Cytograph(selection.metric, g, profile.dataRange);
        let hiddenNodes = new Map<number, HiddenNode>();
        // Maps each hidden node to its representative.
        let hiddenMap = new OMap<NodeId, HiddenNode>();
        let visibleMap = new OMap<NodeId, VisibleNode>();
        let sources = profile.sources.unwrapOr(new Sources([]));

        // Compute the range of the displayed value
        let range = profile.propertyRange(selection.metric);
        let visibleParents = new Set<NodeId>();
        for (let [nodeId, node] of profile.simpleNodes.entries()) {
            let hidden = !selection.nodesVisible.contains(nodeId);
            let d = depth.get(nodeId)!;
            if (hidden) {
                // Create a hidden node for each depth.
                let hiddenNode = hiddenNodes.get(d);
                if (hiddenNode === undefined) {
                    hiddenNode = new HiddenNode(nodeId);
                    hiddenNodes.set(d, hiddenNode);
                }
                hiddenNode.addNode(nodeId);
                hiddenMap.set(nodeId, hiddenNode);
                graph.addNode(hiddenNode);
            } else {
                // Create a visible node

                // Find out whether we display this node or only its parent
                let topParent = profile.getTopParent(nodeId);
                const expand = selection.regionsExpanded.contains(topParent);
                if (!expand && nodeId !== topParent) {
                    if (visibleMap.has(topParent))
                        // Another child has inserted this parent
                        continue;
                    nodeId = topParent;
                    node = profile.complexNodes.get(nodeId).unwrap();
                }

                let percents = 0;
                let attributes = Cytograph.createAttributes(node, selection, profile);
                if (!range.isEmpty() && !range.isPoint()) {
                    let m = node.getMeasurements(selection.metric);
                    m = selection.workersVisible.getSelectedElements(m);
                    let values = m.map(v => v.getNumericValue()).filter(v => v.isSome()).map(v => v.unwrap());
                    let max = Math.max(...values, 0);
                    percents = range.percents(max);
                }

                let parent = profile.parents.get(node.id);
                if (parent.isSome()) {
                    const p = parent.unwrap();
                    visibleParents.add(p);
                }
                let visibleNode = new VisibleNode(nodeId, attributes.kv.get("operation") || "unknown", percents, parent, attributes);
                graph.addNode(visibleNode);
                visibleMap.set(nodeId, visibleNode);
            }
        }

        // If all nodes in a hidden node have the same parent, make that the parent of the hidden node.
        for (const hiddenNode of hiddenNodes.values()) {
            let parents = new Set<Option<string>>();
            for (const nodeId of hiddenNode.assigned) {
                let parent = profile.parents.get(nodeId);
                parents.add(parent);
            }
            if (parents.size === 1) {
                let parent = parents.values().next().value!;
                if (parent.isSome()) {
                    hiddenNode.setParent(parent.unwrap());
                }
            }
        }

        for (const nodeId of profile.complexNodes.keys()) {
            let complex = profile.complexNodes.get(nodeId).unwrap();
            let parent = profile.parents.get(nodeId);
            if (!profile.isTop(nodeId) && visibleParents.has(nodeId)) {
                let positions = complex.sourcePositions;
                let src = sources.toString(positions);
                graph.addNode(new VisibleNode(nodeId, "region", 0, parent,
                    new Attributes([], new Map(), new Map(), src)));
            }
        }

        for (const edge of profile.edges) {
            let source = edge.source;
            let target = edge.target;

            let sourceParent = profile.getTopParent(source);
            let targetParent = profile.getTopParent(target);
            let expandSource = selection.regionsExpanded.contains(sourceParent);
            let expandTarget = selection.regionsExpanded.contains(targetParent);
            if (!expandSource) {
                source = sourceParent;
            }
            if (!expandTarget) {
                target = targetParent;
            }

            let sourceHidden = !selection.nodesVisible.contains(source);
            let targetHidden = !selection.nodesVisible.contains(target);
            if (expandTarget && profile.complexNodes.has(target))
                // Do not add edges to complex nodes.
                continue;

            let sourceNode;
            let targetNode;

            if (sourceHidden) {
                sourceNode = hiddenMap.get(source).expect(`Node ${source} not found in hidden map`);
            } else {
                sourceNode = visibleMap.get(source).expect(`Node ${source} not found in visible map`);
            }
            if (targetHidden) {
                targetNode = hiddenMap.get(target).expect(`Node ${target} not found in hidden map`);
            } else {
                targetNode = visibleMap.get(target).expect(`Node ${target} not found in visible map`);
            }

            if (sourceNode !== targetNode) {
                if (sourceHidden || targetHidden) {
                    // Do not add duplicate edges
                    if (graph.edges.find(
                        e => e.source === sourceNode.id && e.target === targetNode.id) !== undefined) {
                        continue;
                    }
                }
                graph.addEdge(sourceNode.id, targetNode.id, edge.back);
            }
        }
        return graph;
    }

    diff(other: Cytograph): GraphDiff {
        let nodes = ZSet.fromIterator(this.nodes);
        let otherNodes = ZSet.fromIterator(other.nodes);
        let nodeDiff = nodes.except(otherNodes);

        let edges = ZSet.fromIterator(this.edges);
        let otherEdges = ZSet.fromIterator(other.edges);
        let edgeDiff = edges.except(otherEdges);
        return new GraphDiff(nodeDiff, edgeDiff);
    }
}

class GraphDiff {
    constructor(readonly nodes: ZSet<GraphNode>, readonly edges: ZSet<GraphEdge>) { }

    toString(): string {
        let result = this.nodes.toString(n => n.getId());
        result += "\n" + this.edges.toString(e => e.toString());
        return result;
    }
}

/** Handles updates and rendering for the displayed circuit graph. */
export class CytographRendering {
    currentGraph: Cytograph | null;
    readonly cy: cytoscape.Core;
    readonly navigator;

    readonly graph_style: StylesheetJson = [
        {
            selector: 'node',
            css: {
                'shape': 'rectangle',
                'content': 'data(label)',
                'text-valign': 'center',
                'text-halign': 'center',
                'background-color': "mapData(value, 0, 100, white, red)",
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
            selector: 'node[hidden]',
            style: {
                'border-style': 'dotted',
                'border-color': 'black',
                'border-width': '1px',
                'width': 'label',
            }
        },
        {
            selector: 'node[depth]:parent',
            style: {
                'label': '',
                'text-opacity': 0,
                'text-events': 'no',
                'background-color': 'mapData(depth, 0, 5, #f5f5f5, #d0d0d0)',
            }
        },
        {
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
            selector: 'edge.highlight-backward',
            style: {
                'line-color': 'blue',
                'target-arrow-color': 'blue',
                'width': 4
            }
        },
        {
            selector: 'edge.highlight-forward',
            style: {
                'line-color': 'red',
                'target-arrow-color': 'red',
                'width': 4
            }
        },
    ];

    constructor(readonly graph: Graph<NodeId>, readonly selection: CircuitSelection) {
        cytoscape.use(elk);
        cytoscape.use(dblclick);

        let parent = document.getElementById('app')!;
        this.navigator = new ViewNavigator(document.getElementById("navigator-parent")!);
        this.currentGraph = null;
        this.cy = cytoscape({
            container: parent,
            elements: [],
        });
        this.cy.style(this.graph_style);
    }

    readonly layoutOptions = {
        animate: false,
        nodeDimensionsIncludeLabels: true,
        name: 'elk',
        elk: {
            'algorithm': 'layered',
            'elk.direction': 'DOWN',
            'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
        }
    };

    updateGraph(newGraph: Cytograph) {
        if (this.currentGraph === null) {
            this.currentGraph = newGraph;
            this.cy.add(newGraph.getGraphElements());
            this.graphUpdated();
        } else {
            let graphDiff = newGraph.diff(this.currentGraph);
            this.currentGraph = newGraph;
            this.cy!.batch(() => {
                this.applyDiff(this.cy, graphDiff);
                this.graphUpdated();
            });
        }
    }

    graphUpdated() {
        if (this.cy === null)
            return;
        if (this.cy === null) return;
        this.cy.nodes().forEach(n => {
            const depth = n.ancestors().filter(n => n.isNode()).length;
            n.data('depth', depth);
        });
        this.cy
            .elements()
            .layout(this.layoutOptions)
            .run()
    }

    applyDiff(cy: cytoscape.Core, diff: GraphDiff) {
        for (const [node, weight] of diff.nodes.entries()) {
            let def = node.getDefinition();
            if (weight < 0) {
                cy.add(def);
            } else {
                cy.$id(node.getId()).remove();
            }
        }

        for (const [edge, weight] of diff.edges.entries()) {
            let def = edge.getDefinition();
            if (weight < 0) {
                cy.add(def);
            } else {
                let selector = '[source = "' + edge.source + '"][target = "' + edge.target + '"]';
                cy.edges(selector).remove();
            }
        }
    }

    render(onDoubleClick: (node: NodeId) => void) {
        if (this.currentGraph === null)
            return;

        this.graphUpdated();

        this.cy
            // .on('render', () => console.log("rendering"))
            .on('layoutstop', (e) => this.updateNavigator(e, this.navigator))
            .on('mouseover', 'node', event => this.toolTip(event))
            .on('mouseout', 'node', event => this.hideToolTip(event))
            .on('zoom pan resize', (e) => this.updateNavigator(e, this.navigator))
            .on('dblclick', 'node', (e) => this.doubleClick(e, onDoubleClick))
            ;
    }

    doubleClick(e: EventObject, onDoubleClick: (node: NodeId) => void) {
        let id = e.target.id();
        onDoubleClick(id);
    }

    // Display the screen and circuit overlap
    updateNavigator(_e: EventObject, navigator: ViewNavigator) {
        //console.log(e.type.toString());
        if (this.cy === null)
            return;
        const container = this.cy.container();
        if (container === null)
            return;

        let rect = container.getBoundingClientRect();
        const zoom = this.cy.zoom();
        const pan = this.cy.pan();
        const bb = this.cy.elements().boundingBox();
        navigator.setViewParameters(
            new Size(rect.width, rect.height),
            new Point(pan.x, pan.y),
            new Size(bb.w * zoom, bb.h * zoom));
    }

    // Compute the edges reachable to/from a node in the underlying graph
    reachableFrom(node: NodeSingular, forward: boolean): EdgeCollection {
        let id = node.id();
        let result = this.cy!.collection();

        if (forward) {
            let reachable = this.graph.reachableFrom(id, e => !e.back);
            for (let n of reachable) {
                let node = this.cy!.getElementById(n);
                result = result.union(node.outgoers().edges());
            }
        } else {
            let reverseReachable = this.graph.canReach(id, e => !e.back);
            for (let n of reverseReachable) {
                let node = this.cy!.getElementById(n);
                result = result.union(node.incomers().edges());
            }
        }

        return result;
    }

    // Generate a tooltip to display on hover over a node.
    toolTip(event: EventObject) {
        if (this.cy === null)
            return;

        const globals = Globals.getInstance();
        let node: NodeSingular = event.target;
        let reachable = this.reachableFrom(node, true);
        reachable.addClass('highlight-forward');
        reachable = this.reachableFrom(node, false);
        reachable.addClass('highlight-backward');

        let attributes: Attributes = node.data().attributes;
        let table = document.createElement("table");

        let tableWidth = 1;
        let visible = false;
        if (attributes.attributeCount() > 0 && attributes.getColumnCount() > 0) {
            visible = true;
            let row = table.insertRow();
            row.insertCell(0);
            let colCount = attributes.getColumnCount();
            tableWidth = colCount + 1;
            for (let i = 0; i < colCount; i++) {
                const th = document.createElement("th");
                th.innerText = attributes.columnNames[i] || "";
                row.appendChild(th);
            }

            let range = this.currentGraph!.propertyRange.get(this.selection.metric);
            const MAX_CELL_COUNT = 40;
            for (const [key, values] of attributes.getAttributes().entries()) {
                let index = 0;
                for (const value of values) {
                    let position = index % MAX_CELL_COUNT;
                    if (position === 0) {
                        // Every MAX_CELL_COUNT start a new row
                        row = table.insertRow();
                        let cell = row.insertCell(0);
                        if (index === 0) {
                            cell.innerText = key;
                            if (key === this.selection.metric) {
                                cell.style.backgroundColor = "blue";
                            }
                        }
                    }

                    let cell = row.insertCell(position + 1);
                    let v = parseFloat(value);
                    if (range.isSome() && !isNaN(v) &&
                        !range.unwrap().isEmpty() && !range.unwrap().isPoint()) {
                        let percent = range.unwrap().percents(v);
                        let color = `rgb(${255 * percent / 100}, ${255 * ((100 - percent) / 100)}, 0)`
                        cell.style.backgroundColor = color;
                    } else {
                        cell.style.backgroundColor = "white";
                    }
                    cell.style.color = "black";
                    cell.innerText = value;
                    cell.style.textAlign = "right";
                    index++;
                }
            }
        }

        let sources = attributes.getSources();
        if (sources.length > 0) {
            visible = true;
            let row = table.insertRow();
            let cell = row.insertCell(0);
            cell.innerText = "sources";

            cell = row.insertCell(1);
            cell.colSpan = tableWidth - 1;
            cell.style.textAlign = "left";
            cell.style.fontFamily = "monospace";
            cell.style.whiteSpace = "pre-wrap";
            cell.style.minWidth = "80ch";
            cell.innerText = sources;
        }

        if (attributes.kv.size != 0) {
            visible = true;
            for (const kv of attributes.kv.entries()) {
                let row = table.insertRow();
                let cell = row.insertCell(0);
                cell.innerText = kv[0];
                cell.style.whiteSpace = "nowrap";

                cell = row.insertCell(1);
                cell.colSpan = tableWidth - 1;
                cell.style.whiteSpace = "nowrap";
                cell.innerText = kv[1];
            }
        }

        if (!visible)
            return;

        const canvasRect = this.cy.container()!.getBoundingClientRect();
        globals.tooltip.innerHTML = "";  // Clear previous content.
        globals.tooltip.appendChild(table);
        globals.tooltip.style.display = 'block';

        let x, y;
        if (!CytographRendering.FIXED_TOOLTIP_POSITION) {
            x = (canvasRect.left + event.renderedPosition.x);
            y = (canvasRect.top + event.renderedPosition.y);
        } else {
            x = canvasRect.right;
            y = 0;
        }

        // Make sure the table is fully visible.
        const rect = table.getBoundingClientRect();
        if (x + rect.width > canvasRect.width) {
            x -= rect.width + 10;
        }
        if (y + rect.height > canvasRect.height) {
            y -= rect.height;
        }
        // Could become negative if the rendering is too large
        x = Math.max(0, x);
        y = Math.max(0, y);
        globals.tooltip.style.left = x + `px`;
        globals.tooltip.style.top = y + `px`;
    }

    hideToolTip(event: EventObject) {
        const globals = Globals.getInstance();
        globals.tooltip.style.display = 'none';
        let node: NodeSingular = event.target;
        let reachable = this.reachableFrom(node, true);
        reachable.removeClass('highlight-forward');
        reachable = this.reachableFrom(node, false);
        reachable.removeClass('highlight-backward');
    }

    // Set to true to display the tooltip in a fixed position.
    // Set to false to display the tooltip at the mouse position.
    static readonly FIXED_TOOLTIP_POSITION = true;

}