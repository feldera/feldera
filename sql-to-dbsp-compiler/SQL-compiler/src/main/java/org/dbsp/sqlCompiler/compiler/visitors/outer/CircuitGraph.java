package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/* The Graph represents edges source->destination,
 * while the circuit represents edges destination->source. */
public class CircuitGraph {
    public record Port(DBSPOperator operator, int input) {}

    private final Set<DBSPOperator> nodeSet = new HashSet<>();
    private final List<DBSPOperator> nodes = new ArrayList<>();
    private final Map<DBSPOperator, List<Port>> edges = new HashMap<>();

    void addNode(DBSPOperator node) {
        if (this.nodeSet.contains(node))
            return;
        this.nodes.add(node);
        this.nodeSet.add(node);
        this.edges.put(node, new ArrayList<>());
    }

    void addEdge(DBSPOperator source, DBSPOperator dest, int input) {
        assert this.nodeSet.contains(source);
        assert this.nodeSet.contains(dest);
        this.edges.get(source).add(new Port(dest, input));
    }

    @Override
    public String toString() {
        return "CircuitGraph{" +
                "nodes=" + nodes +
                ", edges=" + edges +
                '}';
    }

    public void clear() {
        this.nodeSet.clear();
        this.edges.clear();
        this.nodes.clear();
    }

    public List<Port> getDestinations(DBSPOperator source) {
        return Utilities.getExists(this.edges, source);
    }

    public int getFanout(DBSPOperator operator) {
        return this.getDestinations(operator).size();
    }
}
