package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Build a graph data structure from a Circuit */
public class Graph extends CircuitVisitor {
    /* The Graph represents edges source->destination,
     * while the circuit represents edges destination->source. */
    public static class CircuitGraph {
        final Set<DBSPOperator> nodeSet = new HashSet<>();
        final List<DBSPOperator> nodes = new ArrayList<>();
        final Map<DBSPOperator, List<DBSPOperator>> edges = new HashMap<>();

        void addNode(DBSPOperator node) {
            if (this.nodeSet.contains(node))
                return;
            this.nodes.add(node);
            this.nodeSet.add(node);
            this.edges.put(node, new ArrayList<>());
        }

        void addEdge(DBSPOperator source, DBSPOperator dest) {
            assert this.nodeSet.contains(source);
            assert this.nodeSet.contains(dest);
            this.edges.get(source).add(dest);
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
    }

    public final CircuitGraph graph = new CircuitGraph();

    public Graph(IErrorReporter errorReporter) {
        super(errorReporter);
    }

    @Override
    public void startVisit(IDBSPOuterNode node) {
        this.graph.clear();
    }

    @Override
    public void postorder(DBSPOperator operator) {
        this.graph.addNode(operator);
        for (DBSPOperator source: operator.inputs) {
            this.graph.addEdge(source, operator);
        }
    }
}
