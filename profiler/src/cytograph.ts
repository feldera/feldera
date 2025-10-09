import cytoscape, { type EdgeCollection, type EdgeDefinition, type ElementsDefinition, type EventObject, type NodeDefinition, type NodeSingular, type StylesheetJson } from 'cytoscape';
import { Graph, OMap, Option } from './util.js';
import { Globals } from './globals.js';
import { CircuitProfile, type NodeId } from './profile.js';
import { CircuitSelection } from './navigation.js';
import dagre from 'cytoscape-dagre';

/** Cytoscape attributes to be displayed for a node on hover. */
class Attributes {
    constructor(
        // There should be one column name for each value in the attributes array
        readonly columnNames: Array<string> = [],
        private readonly attributes: Map<string, Array<string>> = new Map()
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
}

interface GraphNode {
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
        readonly attributes: Attributes = new Attributes(),
        readonly persistent_id: Option<string> = Option.none()
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
}

/** A Cytoscape graph node that represents multiple hidden profile nodes. */
class HiddenNode implements GraphNode {
    assigned: Set<NodeId> = new Set();
    parent: Option<string> = Option.none();

    constructor(
        readonly id: string
    ) { }

    addNode(nodeId: NodeId): HiddenNode {
        this.assigned.add(nodeId);
        return this;
    }

    setParent(parent: string): void {
        this.parent = Option.some(parent);
    }

    getDefinition(): NodeDefinition {
        let result = { "data": { "id": this.id, "value": 0, "label": this.assigned.size.toString() + " nodes" } };
        let attributes = new Attributes(
            ["hidden"], new Map([["hidden nodes", Array.from(this.assigned).map(n => VisibleNode.normalizeId(n.toString()))]])
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

    getValue(): EdgeDefinition {
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
    readonly metric: string;
    readonly graph: Graph<NodeId>;
    cy: cytoscape.Core | null = null;

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
            selector: 'node:parent',
            style: {
                'label': '',
                'text-opacity': 0,
                'text-events': 'no',
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
            selector: 'edge.highlight',
            style: {
                'line-color': 'red',
                'target-arrow-color': 'red',
                'width': 4
            }
        },
    ];

    constructor(metric: string, graph: Graph<NodeId>) {
        this.nodes = [];
        this.edges = [];
        this.metric = metric;
        this.graph = graph;
    }

    addNode(node: GraphNode) {
        this.nodes.push(node);
    }

    addEdge(source: string, target: string, back: boolean = false) {
        this.edges.push(new GraphEdge(source, target, back));
    }

    getGraphElements(): ElementsDefinition {
        return {
            "nodes": this.nodes.map(n => n.getDefinition()),
            "edges": this.edges.map(e => e.getValue())
        }
    }

    toString(): string {
        return JSON.stringify(this.nodes) + "\n" + JSON.stringify(this.edges);
    }

    render() {
        let parent = document.getElementById('app')!;
        window.onresize = () => {
            this.render()
        };
        this.cy = cytoscape({
            container: parent,
            elements: this.getGraphElements(),
            style: this.graph_style
        });

        const dagreOptions = {
            name: 'dagre',
            rankDir: 'TB',
            nodeSep: 20,
            rankSep: 10,
            fit: false, // Whether to fit the viewport to the graph
            nodeDimensionsIncludeLabels: true,
        };

        cytoscape.use(dagre);
        this.cy.elements().not('.back').layout(dagreOptions).run();
        this.cy
            .on('mouseover', 'node', event => this.toolTip(event))
            .on('mouseout', 'node', event => this.hideToolTip(event))
            .center()
            .fit();
    }

    // Set to true to display the tooltip in a fixed position.
    // Set to false to display the tooltip at the mouse position.
    static readonly FIXED_TOOLTIP_POSITION = true;

    reachableFrom(node: NodeSingular): EdgeCollection {
        let id = node.id();
        let reachable = this.graph.reachableFrom(id, e => !e.back);
        let reverseReachable = this.graph.canReach(id, e => !e.back);

        let result = this.cy!.collection();
        for (let n of reachable) {
            let node = this.cy!.getElementById(n);
            result = result.union(node.outgoers().edges());
        }
        for (let n of reverseReachable) {
            let node = this.cy!.getElementById(n);
            result = result.union(node.incomers().edges());
        }
        return result;
    }

    // Generate a tooltip to display on hover over a node.
    toolTip(event: EventObject) {
        if (this.cy === null)
            return;

        const globals = Globals.getInstance();
        let node: NodeSingular = event.target;
        let reachable = this.reachableFrom(node);
        reachable.addClass('highlight');

        let attributes: Attributes = node.data().attributes;
        if (attributes.attributeCount() === 0) {
            return;
        }
        let table = document.createElement("table");
        let row = table.insertRow();
        row.insertCell(0);
        let colCount = attributes.getColumnCount();
        for (let i = 0; i < colCount; i++) {
            const th = document.createElement("th");
            th.innerText = attributes.columnNames[i] || "";
            row.appendChild(th);
        }
        for (const [key, values] of attributes.getAttributes().entries()) {
            let row = table.insertRow();
            if (key === this.metric) {
                row.style.backgroundColor = "blue";
            }
            let cell = row.insertCell(0);
            cell.innerText = key;
            let index = 1;
            for (const value of values) {
                cell = row.insertCell(index++);
                cell.innerText = value;
                cell.style.textAlign = "right";
            }
        }
        const canvasRect = this.cy.container()!.getBoundingClientRect();
        globals.tooltip.innerHTML = "";  // Clear previous content.
        globals.tooltip.appendChild(table);
        globals.tooltip.style.display = 'block';

        let x, y;

        if (!Cytograph.FIXED_TOOLTIP_POSITION) {
            x = (canvasRect.left + event.renderedPosition.x);
            y = (canvasRect.top + event.renderedPosition.y);

            // Make sure the table is fully visible.
        } else {
            x = canvasRect.right;
            y = 0;
        }

        const rect = table.getBoundingClientRect();
        if (x + rect.width > window.innerWidth) {
            x -= rect.width;
        }
        if (y + rect.height > window.innerHeight) {
            y -= rect.height;
        }
        globals.tooltip.style.left = x + `px`;
        globals.tooltip.style.top = y + `px`;
    }

    hideToolTip(event: EventObject) {
        const globals = Globals.getInstance();
        globals.tooltip.style.display = 'none';
        let node: NodeSingular = event.target;
        let reachable = this.reachableFrom(node);
        reachable.removeClass('highlight');
    }

    // Test example graph.
    static createTestExample(): Cytograph {
        let graph = new Cytograph("latency", new Graph<NodeId>());
        const workers = ["0", "1"];
        graph.addNode(new VisibleNode("0", "filter", 100, Option.none(),
            new Attributes(workers, new Map([["latency", ["10ms", "20ms"]]]))));
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

    // Create a Cytograph from a CircuitProfile filtered by the specified selection.
    static fromProfile(profile: CircuitProfile, selection: CircuitSelection): Cytograph {
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
        let depth = g.dfs();

        let graph = new Cytograph(selection.metric, g);
        let hiddenNodes = new Map<number, HiddenNode>();
        // Maps each hidden node to its representative.
        let hiddenMap = new OMap<NodeId, HiddenNode>();
        let visibleMap = new OMap<NodeId, VisibleNode>();

        // Scan the nodes and compute the range of the displayed value
        let range = profile.propertyRange(selection.metric);

        let visibleParents = new Set<NodeId>();
        for (const [nodeId, node] of profile.simpleNodes.entries()) {
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
                let workers = selection.workersVisible.getSelectedElements(profile.getWorkerNames());
                let columnNames = workers.map(w => w.toString());
                let data = new Map<string, Array<string>>();
                // Select just the visible metrics
                let persistentId = Option.none<string>();
                for (let metric of node.measurements.getMetrics()) {
                    let metrics = node.getMeasurements(metric);
                    if (metric === CircuitProfile.PERSISTENT_ID && metrics.length > 0)
                        // Set the persistent ID for this node.
                        persistentId = Option.some(metrics[0]!.getStringValue());
                    let selected = selection.workersVisible.getSelectedElements(metrics);
                    data.set(metric, selected.map(m => m.toString()));
                }
                let percents = 0;
                let attributes = new Attributes(columnNames, data);
                if (!range.isEmpty() && !range.isPoint()) {
                    let m = node.getMeasurements(selection.metric);
                    m = selection.workersVisible.getSelectedElements(m);
                    let values = m.map(v => v.getNumericValue()).filter(v => v.isSome()).map(v => v.unwrap());
                    let max = Math.max(...values, 0);
                    percents = range.percents(max);
                }
                let operation = node.operation;
                if (operation === CircuitProfile.Z1_TRACE_OUTPUT)
                    // These nodes were fixed in the profile fixZ1Nodes function.
                    operation = CircuitProfile.Z1_TRACE;
                let parent = profile.parents.get(nodeId);
                if (parent.isSome()) {
                    visibleParents.add(parent.unwrap());
                }
                let visibleNode = new VisibleNode(nodeId, operation, percents, parent, attributes, persistentId);
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
            let parent = profile.parents.get(nodeId);
            if (nodeId !== "n" && visibleParents.has(nodeId)) {
                // This avoids adding the top-level graph region
                graph.addNode(new VisibleNode(nodeId, "region", 0, parent, new Attributes()));
            }
        }

        for (const edge of profile.edges) {
            let source = edge.source;
            let target = edge.target;
            let sourceHidden = !selection.nodesVisible.contains(source);
            let targetHidden = !selection.nodesVisible.contains(target);
            if (profile.complexNodes.has(target))
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
                if (source.includes("106_n23"))
                    console.log(`Adding edge from ${sourceNode.id} to ${targetNode.id}`);
                graph.addEdge(sourceNode.id, targetNode.id, edge.back);
            }
        }
        return graph;
    }
}
