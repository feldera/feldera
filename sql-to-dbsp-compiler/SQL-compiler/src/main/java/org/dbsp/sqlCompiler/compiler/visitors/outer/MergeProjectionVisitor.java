package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;

/**
 * Merge Projection operations into the previous operation if possible.
 * Projections are map operations that have a function with a very simple
 * structure.  The function is analyzed using the `Projection' inner visitor.
 */
public class MergeProjectionVisitor extends CircuitCloneVisitor {
    public MergeProjectionVisitor(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPJoinOperator.class) ||
            source.is(DBSPIncrementalJoinOperator.class)) {
            Projection projection = new Projection(this.errorReporter);
            projection.traverse(operator.getFunction());;
            if (projection.isProjection) {
                DBSPClosureExpression expression = source.getFunction().to(DBSPClosureExpression.class);
                DBSPClosureExpression joinFunction = projection.applyAfter(expression);
                DBSPOperator result = source.withFunction(joinFunction, operator.outputType);
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }
}
