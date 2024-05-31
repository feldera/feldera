package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Optimizes patterns containing projections.
 * You probably don't want to use this visitor directly, consider using
 * OptimizeProjections, which packages this nicely and iterates until convergence.
 * Projections are map operations that have a function with a very simple
 * structure.  The function is analyzed using the 'Projection' inner visitor.
 * - Merge Projection operations into the previous operation if possible.
 *   Done for joins, constants, flatmaps, and some maps.
 * - Swap projection with operations such as Distinct, Integral, Differential, Sum, etc.
 */
public class OptimizeProjectionVisitor extends CircuitCloneVisitor {
    /** If this function returns 'true' the operator can be optimized. */
    protected final Function<DBSPOperator, Boolean> canOptimize;

    public OptimizeProjectionVisitor(IErrorReporter reporter, Function<DBSPOperator, Boolean> canOptimize) {
        super(reporter, false);
        this.canOptimize = canOptimize;
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if ((source.is(DBSPStreamJoinOperator.class) || source.is(DBSPJoinOperator.class)) &&
                // We have to look up the original operator input, not source
                this.canOptimize.apply(operator.input())) {
            DBSPClosureExpression expression = source.getFunction().to(DBSPClosureExpression.class);
            DBSPClosureExpression newFunction = operator.getFunction().to(DBSPClosureExpression.class)
                    .applyAfter(this.errorReporter, expression);
            DBSPOperator result = source.withFunction(newFunction, operator.outputType);
            this.map(operator, result);
            return;
        } else if (source.is(DBSPMapOperator.class)) {
            DBSPClosureExpression expression = source.getFunction().to(DBSPClosureExpression.class);
            DBSPClosureExpression newFunction = operator.getFunction().to(DBSPClosureExpression.class)
                    .applyAfter(this.errorReporter, expression);
            DBSPOperator result = source.withFunction(newFunction, operator.outputType);
            this.map(operator, result);
            return;
        } else if (source.is(DBSPIntegrateOperator.class) ||
                source.is(DBSPDifferentiateOperator.class) ||
                source.is(DBSPDelayOperator.class) ||
                source.is(DBSPNegateOperator.class) ||
                source.is(DBSPSumOperator.class) ||
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
            return;
        }

        Projection projection = new Projection(this.errorReporter);
        projection.traverse(operator.getFunction());
        if (projection.isProjection) {
            if (source.is(DBSPConstantOperator.class)) {
                DBSPExpression newConstant = projection.applyAfter(
                        source.to(DBSPConstantOperator.class).getFunction().to(DBSPZSetLiteral.class));
                DBSPOperator result = source.withFunction(newConstant, operator.outputType);
                this.map(operator, result);
                return;
            } else if (source.is(DBSPFlatMapOperator.class)) {
                DBSPFlatmap function = source.getFunction().as(DBSPFlatmap.class);
                if (function != null && projection.isShuffle()) {
                    function = new DBSPFlatmap(
                            function.getNode(), function.inputElementType,
                            function.collectionExpression, function.leftCollectionIndexes,
                            function.rightProjections, function.emitIteratedElement,
                            function.collectionIndexType, projection.getShuffle());
                    DBSPOperator result = source.withFunction(function, operator.outputType);
                    this.map(operator, result);
                    return;
                }
            }
        }
        super.postorder(operator);
    }
}
