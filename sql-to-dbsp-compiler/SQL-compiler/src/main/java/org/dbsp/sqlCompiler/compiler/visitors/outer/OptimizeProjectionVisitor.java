package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;

/** Optimizes patterns containing projections.
 * - constant followed by projection
 * - flatmap followed by projection
 * - join followed by projection
 * Projections are map operations that have a function with a very simple
 * structure.  The function is analyzed using the 'Projection' visitor. */
public class OptimizeProjectionVisitor extends CircuitCloneVisitor {
    final CircuitGraph graph;

    public OptimizeProjectionVisitor(IErrorReporter reporter, CircuitGraph graph) {
        super(reporter, false);
        this.graph = graph;
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPExpression function = operator.getFunction();
        int inputFanout = this.graph.getFanout(operator.input());
        Projection projection = new Projection(this.errorReporter);
        projection.apply(function);
        if (projection.isProjection) {
            if (source.is(DBSPConstantOperator.class)) {
                DBSPExpression newConstant = projection.applyAfter(
                        source.to(DBSPConstantOperator.class).getFunction().to(DBSPZSetLiteral.class));
                DBSPOperator result = source.withFunction(newConstant, operator.outputType);
                this.map(operator, result);
                return;
            } else if (source.is(DBSPFlatMapOperator.class)) {
                DBSPFlatmap sourceFunction = source.getFunction().as(DBSPFlatmap.class);
                if (sourceFunction != null && projection.isShuffle()) {
                    DBSPExpression newFunction = new DBSPFlatmap(
                            function.getNode(), sourceFunction.inputElementType,
                            sourceFunction.collectionExpression, sourceFunction.leftCollectionIndexes,
                            sourceFunction.rightProjections, sourceFunction.emitIteratedElement,
                            sourceFunction.collectionIndexType, projection.getShuffle());
                    DBSPOperator result = source.withFunction(newFunction, operator.outputType);
                    this.map(operator, result);
                    return;
                }
            } else if (source.is(DBSPJoinOperator.class)
                    || source.is(DBSPStreamJoinOperator.class)
                    || source.is(DBSPAsofJoinOperator.class)) {
                if (inputFanout == 1 && projection.isShuffle()) {
                    // We only do this if the source is a projection, because then the join function
                    // will still have a simple shape.  Subsequent analyses may care about this.
                    DBSPClosureExpression joinFunction = source.getClosureFunction();
                    DBSPExpression newFunction = function.to(DBSPClosureExpression.class)
                            .applyAfter(this.errorReporter, joinFunction);
                    DBSPOperator result = source.withFunction(newFunction, operator.outputType);
                    this.map(operator, result);
                    return;
                }
            }
        }
        super.postorder(operator);
    }
}
