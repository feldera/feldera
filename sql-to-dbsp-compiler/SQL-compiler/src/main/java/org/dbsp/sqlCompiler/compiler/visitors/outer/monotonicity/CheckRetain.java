package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.OperatorPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.graph.Port;

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
        OperatorPort left = retain.left();
        for (Port<DBSPOperator> dest: graph.getSuccessors(left.node())) {
            if (dest.node() == retain)
                continue;
            if (dest.node().is(DBSPIntegrateTraceRetainKeysOperator.class)) {
                throw new InternalCompilerError("Operator " + left + " has two RetainKeys policies:"
                + retain + " and " + dest.node());
            }
        }
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainValuesOperator retain) {
        OperatorPort left = retain.left();
        for (Port<DBSPOperator> dest: graph.getSuccessors(left.node())) {
            if (dest.node() == retain)
                continue;
            if (dest.node().is(DBSPIntegrateTraceRetainValuesOperator.class)) {
                throw new InternalCompilerError("Operator " + left + " has two RetainValues policies"
                + retain + " and " + dest.node());
            }
        }
    }
}
