// Data structures representing which parts of a circuit should be visualized.

import { type SubSet, CompleteSet, ExplicitSubSet, Option } from "./util.js";
import { CircuitProfile, type NodeId } from "./profile.js";

/** Describes which part of a CircuitProfile to display. */
export class CircuitSelection {
    constructor(
        // Node which triggered a recomputation (if any)
        readonly trigger: Option<NodeId>,
        // Region nodes that should be expanded into components in the rendering
        readonly regionsExpanded: SubSet<NodeId>,
    ) { }
}

export class CircuitSelector {
    private readonly allNodeIds: Set<NodeId>;
    // Node which triggered the last change, if any
    private trigger: Option<NodeId> = Option.none();
    private readonly regionsExpanded: Set<NodeId>;
    private onChange: () => void = () => { };

    constructor(private readonly circuit: CircuitProfile) {
        if (circuit.simpleNodes.size < 100)
            this.regionsExpanded = new Set(circuit.complexNodes.keys());
        else
            this.regionsExpanded = new Set();

        let allKeys = new Set(this.circuit.simpleNodes.keys());
        this.allNodeIds = allKeys.union(new Set(this.circuit.complexNodes.keys()));
    }

    changed(): void {
        this.onChange();
    }

    setOnChange(onChange: () => void): void {
        this.onChange = onChange;
    }

    /** Toggle the expansion of the node with the specified Id. */
    toggleExpand(node: NodeId) {
        this.trigger = Option.some(node);
        if (this.regionsExpanded.has(node))
            this.regionsExpanded.delete(node);
        else
            this.regionsExpanded.add(node);
        this.changed();
    }

    getFullSelection(): CircuitSelection {
        return new CircuitSelection(Option.none(), new CompleteSet(this.allNodeIds))
    }

    getSelection(): CircuitSelection {
        return new CircuitSelection(
            this.trigger,
            new ExplicitSubSet(new Set(this.circuit.complexNodes.keys()), this.regionsExpanded));
    }
}