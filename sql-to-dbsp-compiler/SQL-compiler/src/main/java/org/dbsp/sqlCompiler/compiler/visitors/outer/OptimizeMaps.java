package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/**
 * Optimizes patterns containing Map operators.
 * - Merge Maps operations into the previous operation (Map, Join) if possible.
 * - Swap Maps with operations such as Distinct, Integral, Differential, etc.
 * - Marge Maps with subsequent MapIndex operators
 */
public class OptimizeMaps extends CircuitCloneVisitor {
    final CircuitGraph graph;

    public OptimizeMaps(IErrorReporter reporter, CircuitGraph graph) {
        super(reporter, false);
        this.graph = graph;
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPMapOperator.class)) {
            // mapindex(map) = mapindex
            DBSPClosureExpression expression = source.getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.errorReporter, expression);
            DBSPOperator result = new DBSPMapIndexOperator(
                    operator.getNode(), newFunction, operator.getOutputIndexedZSetType(), source.inputs.get(0));
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        int inputFanout = this.graph.getFanout(operator.input());
        if (source.is(DBSPJoinFilterMapOperator.class)) {
            // map(joinfilter) = joinfilter
            DBSPJoinFilterMapOperator jfm = source.to(DBSPJoinFilterMapOperator.class);
            DBSPExpression newMap = operator.getFunction();
            if (jfm.map != null) {
                newMap = operator.getClosureFunction()
                        .applyAfter(this.errorReporter, jfm.map.to(DBSPClosureExpression.class));
            }
            DBSPOperator result = new DBSPJoinFilterMapOperator(
                    jfm.getNode(), operator.getOutputZSetType(), jfm.getFunction(),
                    jfm.filter, newMap, operator.isMultiset, jfm.left(), jfm.right());
            this.map(operator, result);
        } else if ((source.is(DBSPStreamJoinOperator.class) || source.is(DBSPJoinOperator.class)) &&
                // We have to look up the original operator input, not source
                inputFanout == 1) {
            DBSPClosureExpression expression = source.getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.errorReporter, expression);
            DBSPOperator result = source.withFunction(newFunction, operator.outputType);
            this.map(operator, result);
        } else if (source.is(DBSPMapOperator.class)) {
            DBSPClosureExpression expression = source.getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.errorReporter, expression);
            DBSPOperator result = source.withFunction(newFunction, operator.outputType);
            this.map(operator, result);
        } else if ((source.is(DBSPIntegrateOperator.class) && (inputFanout == 1)) ||
                source.is(DBSPDifferentiateOperator.class) ||
                source.is(DBSPDelayOperator.class) ||
                source.is(DBSPNegateOperator.class) ||
                // source.is(DBSPSumOperator.class) ||  // swapping with sum is not sound
                // since it may apply operations like div by 0 to tuples that may never appear
                source.is(DBSPNoopOperator.class)) {
            // For all such operators we can swap them with the map
            List<DBSPOperator> newSources = new ArrayList<>();
            for (DBSPOperator sourceSource: source.inputs) {
                DBSPOperator newProjection = operator.withInputs(Linq.list(sourceSource), true);
                newSources.add(newProjection);
                this.addOperator(newProjection);
            }
            DBSPOperator result = source.withInputs(newSources, true);
            this.map(operator, result, operator != result);
        } else {
            super.postorder(operator);
        }
    }
}
