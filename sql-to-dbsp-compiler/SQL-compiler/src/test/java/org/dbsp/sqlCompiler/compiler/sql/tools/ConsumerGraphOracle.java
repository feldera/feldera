package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

/** Compare the Java graph and the native graph using the same live nodes at every placement. */
public final class ConsumerGraphOracle extends FrozenCircuitObserver {
    private final Graph actual;
    private final RustCircuitPass nativePass;
    private int placements;
    private int nodes;
    private int edges;

    private ConsumerGraphOracle(DBSPCompiler compiler, ObjectNode record) {
        super(compiler, record);
        this.actual = new Graph(compiler);
        this.nativePass = new RustCircuitPass(compiler, this.actual, new OptimizerSnapshots(compiler));
    }

    @Override
    public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {}

    @Override
    public void after(String container, int index, int iteration, CircuitTransform pass,
                      DBSPCircuit circuit, boolean completed) {
        if (!(pass instanceof Graph reference))
            return;
        long names = DBSPVariablePath.nextGeneratedName();
        if (!completed || this.nativePass.apply(circuit) != circuit || names != DBSPVariablePath.nextGeneratedName())
            throw new AssertionError("Graph changed the circuit or generated names");
        if (!reference.graphs.graphs.keySet().equals(this.actual.graphs.graphs.keySet()))
            throw new AssertionError("Consumer graph bodies differ");
        for (var entry : reference.graphs.graphs.entrySet()) {
            var expected = entry.getValue();
            var result = this.actual.graphs.getGraph(entry.getKey());
            var ordered = new ArrayList<DBSPOperator>();
            expected.getNodes().forEach(ordered::add);
            var nodes = result.getNodes().iterator();
            for (var op : ordered) {
                if (!nodes.hasNext() || nodes.next() != op
                        || !expected.getSuccessors(op).equals(result.getSuccessors(op)))
                    throw new AssertionError("Consumer graph differs at " + container + "/" + index
                            + " for operator " + op.id);
                this.nodes++;
                this.edges += expected.getSuccessors(op).size();
            }
            if (nodes.hasNext())
                throw new AssertionError("Unexpected consumer graph nodes");
        }
        this.placements++;
    }

    @Override
    void finish() {
        if (this.placements == 0)
            throw new AssertionError("No graph placements observed");
        this.observation.putNull("failure");
        this.observation.put("placements", this.placements).put("nodes", this.nodes).put("edges", this.edges);
    }

    public static void main(String[] args) throws IOException {
        run(Path.of(args[0]), ConsumerGraphOracle::new);
    }
}
