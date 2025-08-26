package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.util.graph.Port;

/** Check if all GC operators have an obvious operator they apply to */
public class StrayGC extends CircuitWithGraphsVisitor {
    public StrayGC(DBSPCompiler compiler, CircuitGraphs g) {
        super(compiler, g);
    }

    void check(DBSPBinaryOperator operator) {
        // At least one sibling on the left input must contain an integral
        var left = operator.left();
        if (left.operator.is(DBSPInputMapWithWaterlineOperator.class))
            return;
        CircuitGraph graph = this.getGraph();
        for (Port<DBSPOperator> sibling: graph.getSuccessors(left.operator)) {
            DBSPOperator so = sibling.node();
            if (so.is(DBSPSimpleOperator.class)) {
                DBSPSimpleOperator simple = so.to(DBSPSimpleOperator.class);
                if (simple.containsIntegrator) {
                    return;
                }
            } else {
                // TODO: Give up for now
                return;
            }
        }
        throw new InternalCompilerError("Operator " + operator + " does not have a sibling to GC for");
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
        this.check(operator);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        DBSPOperator left = operator.left().operator;
        if (left.is(DBSPAggregateLinearPostprocessRetainKeysOperator.class) ||
            left.is(DBSPChainAggregateOperator.class) ||
            left.is(DBSPAggregateOperator.class) ||
            left.is(DBSPLagOperator.class))
            return;
        this.check(operator);
    }
}
