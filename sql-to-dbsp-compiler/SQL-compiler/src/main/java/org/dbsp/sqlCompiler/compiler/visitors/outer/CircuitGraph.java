package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.util.IHasId;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.ToIndentableString;
import org.dbsp.util.graph.DFSOrder;
import org.dbsp.util.graph.DiGraph;
import org.dbsp.util.graph.Port;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/* The Graph represents edges source->destination,
 * while the circuit represents edges destination->source. */
public class CircuitGraph implements DiGraph<DBSPOperator>, IHasId, ToIndentableString {
    private static long crtId = 0;
    private final long id;
    private final Set<DBSPOperator> nodeSet = new HashSet<>();
    private final List<DBSPOperator> nodes = new ArrayList<>();
    private final Map<DBSPOperator, List<Port<DBSPOperator>>> edges = new HashMap<>();
    /** Circuit whose graph is represented */
    public final ICircuit circuit;

    public CircuitGraph(ICircuit circuit) {
        this.circuit = circuit;
        this.id = crtId++;
    }

    @Override
    public long getId() {
        return this.id;
    }

    void addNode(DBSPOperator node) {
        if (this.nodeSet.contains(node))
            return;
        this.nodes.add(node);
        this.nodeSet.add(node);
        this.edges.put(node, new ArrayList<>());
        Utilities.enforce(this.circuit.contains(node));
    }

    public void addEdge(DBSPOperator source, DBSPOperator dest, int input) {
        if (!this.nodeSet.contains(source)) {
            throw new InternalCompilerError(
                    "Adding edge from node " + source + " to " + dest +
                    " when source is not in the graph.");
        }
        if (!this.nodeSet.contains(dest)) {
            throw new InternalCompilerError(
                    "Adding edge from node " + source + " to " + dest +
                            " when destination is not in the graph.");
        }
        this.edges.get(source).add(new Port<>(dest, input));
    }

    @Override
    public String toString() {
        return "CircuitGraph " + this.id + "(" + this.circuit.getId() + ") {" +
                "nodes=" + this.nodes +
                ", edges=" + this.edges +
                '}';
    }

    public void clear() {
        this.nodeSet.clear();
        this.edges.clear();
        this.nodes.clear();
    }

    @Override
    public Iterable<DBSPOperator> getNodes() {
        return this.nodes;
    }

    public List<Port<DBSPOperator>> getSuccessors(DBSPOperator source) {
        return Utilities.getExists(this.edges, source);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("CircuitGraph ")
                .append(this.id)
                .append("(")
                .append(this.circuit.getId())
                .append(") {")
                .increase();
        for (var node: this.nodes) {
            builder.append(node.toString());
            List<Port<DBSPOperator>> ports = this.edges.get(node);
            if (!ports.isEmpty())
                builder.append("->");
            if (ports.size() > 1) {
                builder.append("[").increase();
            }
            boolean first = true;
            for (Port<DBSPOperator> port: ports) {
                if (!first)
                    builder.append(", ");
                if (port.node().is(DBSPSimpleOperator.class)) {
                    builder.append(port.node().toString());
                } else {
                    builder.append(port.toString());
                }
                first = false;
            }
            builder.newline();
            if (ports.size() > 1) {
                builder.decrease().append("]").newline();
            }
        }
        return builder.decrease().append("}");
    }

    /** Return a topological sort of this graph */
    public Iterable<DBSPOperator> sort() {
        DFSOrder<DBSPOperator> dfs = new DFSOrder<>(this);
        return dfs.reversePost();
    }
}
