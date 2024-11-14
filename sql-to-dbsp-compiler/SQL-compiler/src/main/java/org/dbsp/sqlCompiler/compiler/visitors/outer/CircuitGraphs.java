package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps each ICircuit to its CircuitGraph
 */
public class CircuitGraphs {
    public final Map<ICircuit, CircuitGraph> graphs = new HashMap<>();

    public void clear() {
        this.graphs.clear();
    }

    public CircuitGraph getGraph(ICircuit circuit) {
        return Utilities.getExists(this.graphs, circuit);
    }

    public void newCircuit(ICircuit circuit) {
        Utilities.putNew(this.graphs, circuit, new CircuitGraph(circuit));
    }
}
