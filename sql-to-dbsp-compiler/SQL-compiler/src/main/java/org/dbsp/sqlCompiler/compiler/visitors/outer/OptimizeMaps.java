package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.util.Linq;
import org.dbsp.util.Maybe;

import java.util.ArrayList;
import java.util.List;

/**
 * Optimizes patterns containing Map operators.
 * - Merge Maps operations into the previous operation (Map, Join) if possible.
 * - Swap Maps with operations such as Distinct, Integral, Differential, etc.
 * - Merge Maps with subsequent MapIndex operators
 * - Merge consecutive apply operators
 */
public class OptimizeMaps extends CircuitCloneWithGraphsVisitor {
    /** If true only optimize projections after joins */
    final boolean onlyProjections;

    public OptimizeMaps(DBSPCompiler compiler, boolean onlyProjections, CircuitGraphs graphs) {
        super(compiler, graphs, false);
        this.onlyProjections = onlyProjections;
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (inputFanout != 1) {
            super.postorder(operator);
            return;
        }
        if (source.node().is(DBSPMapOperator.class)) {
            // mapindex(map) = mapindex
            DBSPClosureExpression expression = source.simpleNode().getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.compiler(), expression, Maybe.MAYBE);
            DBSPSimpleOperator result = new DBSPMapIndexOperator(
                    operator.getNode(), newFunction, operator.getOutputIndexedZSetType(), source.node().inputs.get(0));
            this.map(operator, result);
            return;
        } else if (source.node().is(DBSPMapIndexOperator.class)) {
            // mapindex(mapindex) = mapindex
            DBSPClosureExpression sourceFunction = source.simpleNode().getClosureFunction();
            DBSPClosureExpression thisFunction = operator.getClosureFunction();
            if (thisFunction.parameters.length != 1)
                throw new InternalCompilerError("Expected closure with 1 parameter", operator);
            DBSPExpression argument = new DBSPRawTupleExpression(
                    sourceFunction.body.field(0).borrow(),
                    sourceFunction.body.field(1).borrow());
            DBSPExpression apply = thisFunction.call(argument);
            DBSPClosureExpression newFunction = apply.closure(sourceFunction.parameters)
                    .reduce(this.compiler()).to(DBSPClosureExpression.class);
            DBSPSimpleOperator result = new DBSPMapIndexOperator(
                    operator.getNode(), newFunction, operator.getOutputIndexedZSetType(), source.node().inputs.get(0));
            this.map(operator, result);
            return;
        } else {
            Projection projection = new Projection(this.compiler());
            projection.apply(operator.getFunction());
            if (!this.onlyProjections || projection.isProjection) {
                if (source.node().is(DBSPJoinOperator.class)
                        || source.node().is(DBSPStreamJoinOperator.class)) {
                    DBSPSimpleOperator result = OptimizeProjectionVisitor.mapIndexAfterJoin(
                            this.compiler(), source.node().to(DBSPJoinBaseOperator.class), operator);
                    this.map(operator, result);
                    return;
                } else if (source.node().is(DBSPJoinIndexOperator.class)
                        || source.node().is(DBSPStreamJoinIndexOperator.class)) {
                    DBSPSimpleOperator result = OptimizeProjectionVisitor.mapIndexAfterJoinIndex(
                            this.compiler(), source.node().to(DBSPJoinBaseOperator.class), operator);
                    this.map(operator, result);
                    return;
                }
            }
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (source.node().is(DBSPMapIndexOperator.class)) {
            // deindex(mapindex) = nothing
            this.map(operator, source.node().inputs.get(0).node().to(DBSPSimpleOperator.class));
            return;
        }
        super.postorder(operator);
    }

    public void postorder(DBSPApplyOperator operator) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (source.node().is(DBSPApplyOperator.class) && inputFanout == 1) {
            DBSPApplyOperator apply = source.node().to(DBSPApplyOperator.class);
            // apply(apply) = apply
            DBSPClosureExpression expression = apply.getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.compiler(), expression, Maybe.YES);
            DBSPSimpleOperator result = new DBSPApplyOperator(
                    operator.getNode(), newFunction, operator.outputType,
                    apply.inputs.get(0), apply.comment);
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        Projection projection = new Projection(this.compiler());
        projection.apply(operator.getFunction());
        if (source.node().is(DBSPJoinFilterMapOperator.class) && inputFanout == 1) {
            if (!this.onlyProjections || projection.isProjection) {
                // map(joinfilter) = joinfilter
                DBSPJoinFilterMapOperator jfm = source.node().to(DBSPJoinFilterMapOperator.class);
                DBSPExpression newMap = operator.getFunction();
                if (jfm.map != null) {
                    newMap = operator.getClosureFunction()
                            .applyAfter(this.compiler(), jfm.map.to(DBSPClosureExpression.class), Maybe.YES);
                }
                DBSPSimpleOperator result = new DBSPJoinFilterMapOperator(
                        jfm.getNode(), operator.getOutputZSetType(), jfm.getFunction(),
                        jfm.filter, newMap, operator.isMultiset, jfm.left(), jfm.right())
                        .copyAnnotations(operator).copyAnnotations(source.node()).to(DBSPSimpleOperator.class);
                this.map(operator, result);
                return;
            }
        } else if ((source.node().is(DBSPStreamJoinOperator.class) ||
                source.node().is(DBSPAsofJoinOperator.class) ||
                source.node().is(DBSPJoinOperator.class)) &&
                // We have to look up the original operator input, not source
                inputFanout == 1) {
            if (!this.onlyProjections || projection.isProjection) {
                DBSPSimpleOperator result = OptimizeProjectionVisitor.mapAfterJoin(
                        this.compiler(), source.node().to(DBSPJoinBaseOperator.class), operator);
                this.map(operator, result);
                return;
            }
        } else if (source.node().is(DBSPJoinIndexOperator.class) ||
                   source.node().is(DBSPStreamJoinIndexOperator.class) && inputFanout == 1) {
            if (!this.onlyProjections || projection.isProjection) {
                DBSPSimpleOperator result = OptimizeProjectionVisitor.mapAfterJoinIndex(
                        this.compiler(), source.node().to(DBSPJoinBaseOperator.class), operator);
                this.map(operator, result);
                return;
            }
        } else if (source.node().is(DBSPMapOperator.class) && inputFanout == 1) {
            DBSPClosureExpression expression = source.simpleNode().getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.compiler(), expression, Maybe.MAYBE);
            DBSPSimpleOperator result = source.simpleNode().withFunction(newFunction, operator.outputType);
            this.map(operator, result);
            return;
        } else if ((source.node().is(DBSPIntegrateOperator.class) && (inputFanout == 1)) ||
                source.node().is(DBSPDifferentiateOperator.class) ||
                source.node().is(DBSPDelayOperator.class) ||
                source.node().is(DBSPNegateOperator.class) ||
                (source.node().is(DBSPSumOperator.class) && projection.isProjection) ||
                (source.node().is(DBSPSubtractOperator.class) && projection.isProjection) ||
                // swapping arbitrary maps with sum is not sound
                // since it may apply operations like div by 0 to tuples that may never appear
                source.node().is(DBSPNoopOperator.class)) {
            // For all such operators we can swap them with the map
            List<OutputPort> newSources = new ArrayList<>();
            for (OutputPort sourceSource: source.node().inputs) {
                DBSPSimpleOperator newProjection = operator.withInputs(Linq.list(sourceSource), true);
                newSources.add(newProjection.outputPort());
                this.addOperator(newProjection);
            }
            DBSPSimpleOperator result = source.simpleNode().withInputs(newSources, true);
            this.map(operator, result, operator != result);
            return;
        }
        super.postorder(operator);
    }
}
