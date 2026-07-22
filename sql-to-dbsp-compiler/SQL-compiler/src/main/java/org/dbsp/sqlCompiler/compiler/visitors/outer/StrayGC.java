package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IContainsIntegrator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.util.graph.Port;

/** Check if all GC operators have an obvious operator they apply to */
public class StrayGC extends CircuitWithGraphsVisitor {
    public StrayGC(DBSPCompiler compiler, CircuitGraphs g) {
        super(compiler, g);
    }

    /** Check that the retain operator invokes the runtime function variant that
     * matches its data source: input tables require the non-accumulate variant,
     * everything else the accumulate_ one.  The wrong variant makes the
     * operator a silent no-op. */
    void checkAccumulate(DBSPBinaryOperator operator, boolean accumulate) {
        boolean input = operator.left().operator.is(DBSPInputMapWithWaterlineOperator.class);
        if (input && accumulate)
            throw new InternalCompilerError(
                    "Operator " + operator + " garbage-collects an input table " +
                    "and must use the non-accumulate variant");
        if (!input && !accumulate)
            throw new InternalCompilerError(
                    "Operator " + operator + " garbage-collects a trace inside the " +
                    "accumulate framework and must use the accumulate_ variant");
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
                if (simple.is(IContainsIntegrator.class)) {
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
        // This operator always uses the accumulate_ variant
        this.checkAccumulate(operator, true);
        this.check(operator);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
        // This operator always uses the accumulate_ variant
        this.checkAccumulate(operator, true);
        this.check(operator);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        this.checkAccumulate(operator, operator.accumulate);
        DBSPOperator left = operator.left().operator;
        if (left.is(DBSPAggregateLinearPostprocessRetainKeysOperator.class) ||
            left.is(DBSPChainAggregateOperator.class) ||
            left.is(DBSPAggregateOperator.class) ||
            left.is(DBSPLagOperator.class))
            return;
        this.check(operator);
    }
}
