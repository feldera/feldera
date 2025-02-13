package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.graph.Port;

import java.util.ArrayList;
import java.util.List;

/** Replaces the following pattern:
 * {@link org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator} followed by
 * {@link org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator} by a
 * {@link org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator} operator
 * followed by the same integrator.
 *
 * <P>This is harder than it looks, because the operators in the result graph must be
 * sorted in topological order, which may not be the same as in the original graph.
 * In the result graph there is an edge from the waterline of the RetainKeys operator
 * to the Aggregate operator, which does not exist in the original graph. */
public class LinearPostprocessRetainKeys implements CircuitTransform, IWritesLogs {
    final DBSPCompiler compiler;

    public LinearPostprocessRetainKeys(DBSPCompiler compiler) {
        this.compiler = compiler;
    }

    /** Does the actual replacement work */
    static class ReplaceLinear extends CircuitCloneWithGraphsVisitor {
        ReplaceLinear(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs, false);
        }

        @Override
        public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
            List<Port<DBSPOperator>> destinations = this.getGraph().getSuccessors(operator);
            for (Port<DBSPOperator> port : destinations) {
                if (port.node().is(DBSPIntegrateTraceRetainKeysOperator.class)) {
                    DBSPIntegrateTraceRetainKeysOperator integrator = port.node()
                            .to(DBSPIntegrateTraceRetainKeysOperator.class);
                    OutputPort left = this.mapped(operator.input());
                    // Because we sorted the graph we know that this has already been processed
                    OutputPort right = this.mapped(integrator.right());
                    DBSPAggregateLinearPostprocessRetainKeysOperator replacement =
                            new DBSPAggregateLinearPostprocessRetainKeysOperator(operator.getRelNode(),
                                    operator.getOutputIndexedZSetType(),
                                    operator.getFunction(),
                                    operator.postProcess,
                                    integrator.getClosureFunction(),
                                    left, right);
                    this.map(operator, replacement);
                    return;
                }
            }
            super.postorder(operator);
        }
    }

    @Override
    public String getName() {
        return "LinearPostprocessRetainKeys";
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        Graph graphs = new Graph(this.compiler);
        graphs.apply(circuit);
        CircuitGraph graph = graphs.graphs.getGraph(circuit);

        // We are not doing recursive components now
        List<Pair<DBSPOperator, DBSPOperator>> toAdd = new ArrayList<>();
        for (DBSPOperator op: circuit.allOperators) {
            List<Port<DBSPOperator>> destinations = graph.getSuccessors(op);
            if (op.is(DBSPAggregateLinearPostprocessOperator.class)) {
                for (Port<DBSPOperator> port : destinations) {
                    if (port.node().is(DBSPIntegrateTraceRetainKeysOperator.class)) {
                        DBSPIntegrateTraceRetainKeysOperator integrator = port.node()
                                .to(DBSPIntegrateTraceRetainKeysOperator.class);
                        // Place a dependence in the graph between the integrator's right input
                        // and the aggregate node.  We cannot modify the graph while we iterate on it.
                        toAdd.add(new Pair<>(integrator.right().node(), op));
                    }
                }
            }
        }

        for (var p: toAdd)
            graph.addEdge(p.left, p.right, 0);
        circuit.resort(graph);

        graphs.apply(circuit);
        ReplaceLinear replace = new ReplaceLinear(this.compiler, graphs.getGraphs());
        return replace.apply(circuit);
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
