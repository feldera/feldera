// Data structures representing which parts of a circuit should be visualized.

import { type SubSet, CompleteSet, ExplicitSubSet } from "./util.js";
import { CircuitProfile, type NodeId } from "./profile.js";

/** Describes which part of a CircuitProfile to display. */
export class CircuitSelection {
    constructor(
        // Region nodes that should be expanded into components in the rendering
        readonly regionsExpanded: SubSet<NodeId>,
    ) { }
}

export class CircuitSelector {
    private readonly allNodeIds: Set<NodeId>;
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

    setOnChange(onChange: () => void): void {
        this.onChange = onChange;
    }

    /** Toggle the expansion of the node with the specified Id. */
    toggleExpand(node: NodeId) {
        if (this.regionsExpanded.has(node))
            this.regionsExpanded.delete(node);
        else
            this.regionsExpanded.add(node);
        this.onChange();
    }

    getFullSelection(): CircuitSelection {
        return new CircuitSelection(new CompleteSet(this.allNodeIds))
    }

    getSelection(): CircuitSelection {
        return new CircuitSelection(
            new ExplicitSubSet(new Set(this.circuit.complexNodes.keys()), this.regionsExpanded));
    }
}