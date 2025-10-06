import { fail, toNumber, Option, OMap, type Comparable, assert, type SubSet, CompleteSet, NumericRange, SubList, ExplicitSubSet } from "./util.js";

type JsonMeasurement = Array<any>;
export type NodeId = string;

// Serialized representation of the measurements for a node.
interface JsonProfileEntries {
    entries: Array<JsonMeasurement>;
}

// Serialized representation of a single profile.
interface JsonProfile {
    // Map node name to measurements.
    readonly metadata: Map<NodeId, JsonProfileEntries>;
}

// Serialized representation of a simple circuit node.
interface JsonSimpleCircuitNode {
    id: NodeId;
    label: string;
}

interface JsonSimpleNodeWrapper {
    Simple: JsonSimpleCircuitNode;
}

interface JsonClusterWrapper {
    Cluster: JsonCircuitCluster;
}

// Serialized representation of a cluster of nodes.
interface JsonCircuitCluster {
    id: NodeId;
    label: string;
    nodes: Array<JsonSimpleNodeWrapper | JsonClusterWrapper>;
}

// Serialized representation of an edge.
interface JsonCircuitEdge {
    from_node: NodeId;
    to_node: NodeId;
    from_cluster: boolean;
    to_cluster: boolean;
}

// Serialized representation of a circuit graph.
interface JsonGraph {
    readonly nodes: JsonCircuitCluster;
    readonly edges: Array<JsonCircuitEdge>;
}

// Serialized JSON representation for the
// profiles received from the Feldera pipeline manager.
export interface JsonProfiles {
    readonly worker_profiles: Array<JsonProfile>;
    readonly graph: JsonGraph;
}

///////////////// Parsing code to decode measurements

abstract class PropertyValue implements Comparable<PropertyValue> {
    abstract getNumericValue(): Option<number>;

    getStringValue(): string {
        return this.toString();
    }

    compareTo(other: PropertyValue): number {
        let v1 = this.getNumericValue();
        let v2 = other.getNumericValue();
        if (v1.isSome() && v2.isSome()) {
            return v1.unwrap() - v2.unwrap();
        } else if (v1.isSome()) {
            return 1;
        } else if (v2.isSome()) {
            return -1;
        } else {
            return 0;
        }
    }

    max(other: PropertyValue): PropertyValue {
        if (this.compareTo(other) >= 0) {
            return this;
        } else {
            return other;
        }
    }

    toString(): string {
        let v = this.getNumericValue();
        if (v.isSome()) {
            return v.unwrap().toLocaleString('en-US', { maximumFractionDigits: 2 });
        } else {
            return "N/A";
        }
    }

    abstract plus(other: PropertyValue): PropertyValue;
}

class PercentValue extends PropertyValue {
    readonly numerator: number;
    readonly denominator: number;

    constructor(numerator: any, denominator: any) {
        super();
        this.numerator = toNumber(numerator);
        this.denominator = toNumber(denominator);
    }

    getNumericValue(): Option<number> {
        if (this.denominator === 0) {
            return Option.some(0);
        }
        return Option.some(this.numerator / this.denominator);
    }

    override toString(): string {
        let v = this.getNumericValue();
        if (v.isSome()) {
            return (v.unwrap() * 100).toFixed(2);
        } else {
            return "N/A";
        }
    }

    plus(other: PropertyValue): PropertyValue {
        if (other instanceof PercentValue) {
            if (this.denominator !== other.denominator) {
                throw new Error("Cannot add PercentValue with different denominators");
            }
            return new PercentValue(this.numerator + other.numerator, this.denominator);
        }
        throw new Error("Cannot add PercentValue to " + other);
    }
}

class NumberValue extends PropertyValue {
    readonly value: number;

    constructor(value: any) {
        super();
        this.value = toNumber(value);
    }

    getNumericValue(): Option<number> {
        return Option.some(this.value);
    }

    plus(other: PropertyValue): PropertyValue {
        if (other instanceof NumberValue) {
            return new NumberValue(this.value + other.value);
        }
        throw new Error("Cannot add NumberValue to " + other);
    }
}

class StringValue extends PropertyValue {
    readonly value: string;

    constructor(id: any) {
        super();
        if (typeof id !== "string") {
            throw new TypeError(`Expected a valid string, got: ${id}`);
        }
        this.value = id;
    }

    getNumericValue(): Option<number> {
        return Option.none();
    }

    override compareTo(other: PropertyValue): number {
        let v1 = this.value;
        if (other instanceof StringValue) {
            let v2 = other.value;
            if (v1 < v2) return -1;
            if (v1 > v2) return 1;
            return 0;
        }
        return super.compareTo(other);
    }

    override plus(other: PropertyValue): PropertyValue {
        if (other instanceof StringValue) {
            return new StringValue(this.value + other.value);
        }
        throw new Error("Cannot add StringValue to " + other);
    }

    override getStringValue(): string {
        return this.value;
    }
}

class TimeValue extends PropertyValue {
    constructor(readonly milliseconds: number) {
        super();
    }

    static fromSecondsNanos(secs: any, nanos: any) {
        return new TimeValue(toNumber(secs) * 1000 + toNumber(nanos) / 1000000);
    }

    getNumericValue(): Option<number> {
        return Option.some(this.milliseconds);
    }

    plus(other: PropertyValue): PropertyValue {
        if (other instanceof TimeValue) {
            return new TimeValue(this.milliseconds + other.milliseconds);
        }
        throw new Error("Cannot add TimeValue to " + other);
    }
}

// Decoded measurement value.
export class Measurement {
    readonly property: string;
    readonly value: Option<PropertyValue>;

    constructor(json: JsonMeasurement) {
        this.property = json[0] as string;
        this.value = this.parsePropertyValue(this.property, json.slice(1));
    }

    private parsePropertyValue(prop: string, value: Array<any>): Option<PropertyValue> {
        switch (prop) {
            case "time%":
            case "merge reduction":
                return Option.some(new PercentValue(value[0][0], value[0][1]));
            case "total size":
            case "invocations":
            case "allocated bytes":
            case "used bytes":
            case "shared bytes":
            case "batches":
            case "storage size":
            case "merging batches":
            case "merging size":
            case "allocations":
                return Option.some(new NumberValue(value[0]));
            case "exchange wait time":
            case "merge backpressure wait":
            case "time":
                return Option.some(TimeValue.fromSecondsNanos(value[0].secs, value[0].nanos));
            case "persistent_id":
                return Option.some(new StringValue(value[0]));
            default:
                // Ignore unknown properties.
                return Option.none();
        }
    }
}

// A set of measurements for a single node and many workers.
class Measurements {
    readonly measurements: OMap<string, Array<PropertyValue>> = new OMap();

    add(worker: number, m: Measurement) {
        if (m.value.isNone()) return;  // Ignore unknown properties.
        let value = m.value.unwrap().getNumericValue();
        if (value.isNone()) return;  // Ignore non-numeric properties.

        let meas = this.measurements.get(m.property);
        let arr: Array<PropertyValue>;
        if (meas.isNone()) {
            arr = [];
            meas = Option.some(arr);
            this.measurements.set(m.property, meas.unwrap());
        } else {
            arr = meas.unwrap();
        }
        arr[worker] = m.value.unwrap();
    }

    getMetrics(): Iterable<string> {
        return this.measurements.keys();
    }

    append(other: Measurements) {
        for (const [key, values] of other.measurements.entries()) {
            let existing = this.measurements.get(key);
            if (existing.isNone()) {
                this.measurements.set(key, values);
            } else {
                let arr = existing.unwrap();
                assert(arr.length === values.length, "Mismatched measurement lengths");
                for (let i = 0; i < values.length; i++) {
                    arr[i] = arr[i]!.plus(values[i]!);
                }
            }
        }
    }
}

export class SimpleNode implements JsonSimpleCircuitNode {
    // One measurement set per worker
    readonly measurements: Measurements;
    readonly operation: string;
    readonly sourcePosition: Option<string>;

    constructor(readonly id: NodeId, readonly label: string) {
        this.measurements = new Measurements();
        let parts = label.split('@');
        this.operation = parts[0]!;
        this.sourcePosition = parts[1] ? Option.some(parts[1]) : Option.none();
    }

    addMeasurement(m: Measurement, worker: number) {
        this.measurements.add(worker, m);
    }

    getMeasurements(property: string): Array<PropertyValue> {
        return this.measurements.measurements.get(property).unwrapOr([]);
    }

    append(measurements: Measurements) {
        this.measurements.append(measurements);
    }
}

export class ProfileEdge {
    constructor(readonly source: NodeId, readonly target: NodeId, readonly back: boolean) { }
}

export class CircuitProfile {
    public readonly simpleNodes: OMap<NodeId, SimpleNode> = new OMap();
    // Source, destination, backedge
    public readonly edges: Array<ProfileEdge> = new Array();
    public readonly complexNodes: Set<NodeId> = new Set();
    public readonly parents: OMap<NodeId, NodeId> = new OMap();
    // Set of all metrics found in the profile.
    private allMetrics: Set<string> = new Set();
    private workerNames: Array<number> = new Array();

    addNode(n: JsonSimpleNodeWrapper | JsonClusterWrapper, parent: Option<NodeId>) {
        if ('Simple' in n) {
            let node = n.Simple;
            this.simpleNodes.set(node.id, new SimpleNode(node.id, node.label));
            if (parent.isSome()) {
                this.parents.set(node.id, parent.unwrap());
            }
        } else {
            let node = n.Cluster;
            this.complexNodes.add(node.id);
            for (const child of node.nodes) {
                this.addNode(child, Option.some(node.id));
            }
            if (parent.isSome()) {
                this.parents.set(node.id, parent.unwrap());
            }
        }
    }

    static readonly Z1_TRACE = "Z1 (trace)";
    static readonly Z1_TRACE_OUTPUT = "Z1 (trace) (output)";
    // Name of the property that holds the persistent ID.
    // In the profile graph this is represented in all workers (if present)
    public static readonly PERSISTENT_ID = "persistent_id";

    addEdge(e: JsonCircuitEdge) {
        // Some edges are reversed in the profile.
        // For example, between Z1 (trace) and Z1 (trace) (output)
        let from = e.from_node;
        let to = e.to_node;
        let source = this.getNode(from);
        let target = this.getNode(to);
        let back = false;
        if (source.isSome() && target.isSome()) {
            if (source.unwrap().label.includes(CircuitProfile.Z1_TRACE_OUTPUT) &&
                target.unwrap().label.includes(CircuitProfile.Z1_TRACE)) {
                let tmp = from;
                from = to;
                to = tmp;
                back = true;
            }
        }

        this.edges.push(new ProfileEdge(from, to, back));
    }

    getWorkerNames(): Array<number> {
        return this.workerNames;
    }

    getNode(id: NodeId): Option<SimpleNode> {
        return this.simpleNodes.get(id);
    }

    constructor() { }

    static fromJson(json: JsonProfiles): CircuitProfile {
        let result = new CircuitProfile();
        // Decode the graph structure and create the nodes.
        // The graph itself is always a complex node.
        result.complexNodes.add(json.graph.nodes.id);
        for (const nodeWrapper of json.graph.nodes.nodes) {
            // Ignore top-level graph region
            result.addNode(nodeWrapper, Option.none());
        }
        for (const edge of json.graph.edges) {
            result.addEdge(edge);
        }

        // Decode the profile information and attach to the nodes.
        for (const [index, profile] of json.worker_profiles.entries()) {
            const decoded: Map<NodeId, Array<Measurement>> = result.decodeProfile(profile);
            for (const [node, measurements] of decoded.entries()) {
                // This should be the same for all nodes
                let n: SimpleNode;
                if (result.simpleNodes.has(node)) {
                    n = result.simpleNodes.get(node).unwrap();
                    for (const m of measurements) {
                        n.addMeasurement(m, index);
                    }
                } else if (result.complexNodes.has(node)) {
                    // Ignore measurements for complex nodes.
                } else {
                    fail("Node not found " + node);
                }
            }
        }
        result.workerNames = Array.from({ length: json.worker_profiles.length }, (_, i) => i);
        // Merge Z1 (trace) nodes; they are artificially split in the profile.
        result.fixZ1Nodes();
        return result;
    }

    fixZ1Nodes() {
        // A Z1 node is represented as a "Z1 (trace)" node followed by a "Z1 (trace) (output)" node.
        // We have already flipped the edges to that there's a back-edge from Z1 to Z1 (output).
        // Now we remove the Z1 node, redirect all its incoming edges to Z1 (output),
        // and merge the measurements into the Z1 (output).
        const edges = new Array<ProfileEdge>();

        let backEdges = this.edges.filter(e => e.back);
        let toReplace = new Map<string, string>(backEdges.map(e => [e.source, e.target]));

        for (const edge of this.edges) {
            if (edge.back) {
                let sourceNode = this.simpleNodes.get(edge.source).unwrap();
                let targetNode = this.simpleNodes.get(edge.target).unwrap();
                targetNode.append(sourceNode.measurements);
                assert(toReplace.has(edge.source), "Expected to delete node " + edge.source);
                this.simpleNodes.delete(edge.source);
            } else {
                if (toReplace.has(edge.target)) {
                    // Redirect edge to the target of the back edge.
                    let redirect = new ProfileEdge(edge.source, toReplace.get(edge.target)!, true);
                    edges.push(redirect);
                } else {
                    edges.push(edge);
                }
            }
        }
        this.edges.length = 0;
        for (const edge of edges) {
            this.edges.push(edge);
        }
    }

    // Given a property, compute the range of values across all nodes.
    propertyRange(property: string): NumericRange {
        // Scan the nodes and compute the range of the displayed value
        let range = NumericRange.empty();
        for (const node of this.simpleNodes.values()) {
            let m = node.getMeasurements(property);
            let values = m.map(v => v.getNumericValue()).filter(v => v.isSome()).map(v => v.unwrap());
            range = range.union(NumericRange.getRange(values));
        }
        return range;
    }

    nodesAboveThreshold(property: string, workers: SubList, percentage: number): SubSet<NodeId> {
        let fullSet = new CompleteSet(this.simpleNodes.keys());
        let range = this.propertyRange(property);
        if (range.isEmpty() || range.isPoint()) {
            return fullSet
        }

        let selected: Set<NodeId> = new Set<NodeId>();
        let threshold = range.quantile(percentage);

        for (const node of this.simpleNodes.values()) {
            let m = node.getMeasurements(property);
            m = m.filter((_, i) => workers.contains(i));
            let values = m.map(v => v.getNumericValue()).filter(v => v.isSome()).map(v => v.unwrap());
            let nodeRange = NumericRange.getRange(values);
            if (nodeRange.max >= threshold) {
                selected.add(node.id);
            }
        }

        return new ExplicitSubSet(fullSet.fullSet, selected);
    }

    private decodeProfile(json: JsonProfile): Map<NodeId, Array<Measurement>> {
        let metadata = new Map<NodeId, Array<Measurement>>();
        for (const [key, measurements] of Object.entries(json.metadata)) {
            let parsed = [];
            for (const m of measurements.entries) {
                let raw = new Measurement(m);
                if (raw.value.isSome()) {  // Ignore unknown properties.
                    parsed.push(raw);
                    this.allMetrics.add(raw.property);
                }
            }
            metadata.set(key, parsed);
        }
        return metadata;
    }
}

