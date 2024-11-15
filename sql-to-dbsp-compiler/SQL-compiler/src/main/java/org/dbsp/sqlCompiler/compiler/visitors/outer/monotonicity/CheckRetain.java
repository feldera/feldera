package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.util.graph.Port;

/** The DBSP runtime will incorrectly GC a relation that has multiple Retain operators of
 * the same kind.  Check that this doesn't happen. */
public class CheckRetain extends CircuitWithGraphsVisitor {
    public CheckRetain(IErrorReporter errorReporter, CircuitGraphs graphs) {
        super(errorReporter, graphs);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator retain) {
        OutputPort left = retain.left();
        for (Port<DBSPOperator> dest: this.getGraph().getSuccessors(left.node())) {
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
        OutputPort left = retain.left();
        for (Port<DBSPOperator> dest: this.getGraph().getSuccessors(left.node())) {
            if (dest.node() == retain)
                continue;
            if (dest.node().is(DBSPIntegrateTraceRetainValuesOperator.class)) {
                throw new InternalCompilerError("Operator " + left + " has two RetainValues policies"
                + retain + " and " + dest.node());
            }
        }
    }
}
