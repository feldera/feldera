package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.ToIndentableString;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import javax.annotation.Nullable;

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

    /** The graph of the root circuit, which contains the recursive circuits */
    CircuitGraph rootGraph() {
        for (CircuitGraph graph : this.graphs.values())
            if (graph.circuit.is(DBSPCircuit.class))
                return graph;
        throw new InternalCompilerError("No graph for the root circuit");
    }

    /** The operator closest downstream of {@code start}
     * that satisfies {@code test}; null if none.  When {@code circuit} is a recursive
     * circuit that contains no such operator, the search continues in the root
     * circuit from the successors of the recursive circuit. */
    @Nullable
    public DBSPOperator closestDownstream(
        ICircuit circuit, DBSPOperator start, Predicate<DBSPOperator> test) {
        DBSPOperator found = this.getGraph(circuit).closestSuccessor(start, test);
        if (found == null && circuit.is(DBSPNestedOperator.class))
            found = this.rootGraph().closestSuccessor(circuit.to(DBSPNestedOperator.class), test);
        return found;
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
