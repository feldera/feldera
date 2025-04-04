package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.ToIndentableString;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Maps each ICircuit to its {@link CircuitGraph} */
public class CircuitGraphs implements ToIndentableString {
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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (var graph: this.graphs.values()) {
            builder.append(graph.toString()).append("\n");
        }
        return builder.toString();
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("{").increase();
        for (var graph: this.graphs.values()) {
            builder.append(graph)
                    .append("\n");
        }
        return builder.decrease().append("}");
    }
}
