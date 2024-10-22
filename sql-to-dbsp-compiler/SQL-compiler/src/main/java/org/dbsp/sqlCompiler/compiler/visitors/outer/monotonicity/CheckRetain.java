package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;

/** The DBSP runtime will incorrectly GC a relation that has multiple Retain operators of
 * the same kind.  Check that this doesn't happen. */
public class CheckRetain extends CircuitVisitor {
    final CircuitGraph graph;

    public CheckRetain(IErrorReporter errorReporter, CircuitGraph graph) {
        super(errorReporter);
        this.graph = graph;
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator retain) {
        DBSPOperator left = retain.left();
        for (CircuitGraph.Port dest: graph.getDestinations(left)) {
            if (dest.operator() == retain)
                continue;
            if (dest.operator().is(DBSPIntegrateTraceRetainKeysOperator.class)) {
                throw new InternalCompilerError("Operator " + left + " has two RetainKeys policies:"
                + retain + " and " + dest.operator());
            }
        }
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainValuesOperator retain) {
        DBSPOperator left = retain.left();
        for (CircuitGraph.Port dest: graph.getDestinations(left)) {
            if (dest.operator() == retain)
                continue;
            if (dest.operator().is(DBSPIntegrateTraceRetainValuesOperator.class)) {
                throw new InternalCompilerError("Operator " + left + " has two RetainValues policies"
                + retain + " and " + dest.operator());
            }
        }
    }
}
