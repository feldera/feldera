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
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/**
 * Optimizes patterns containing Map operators.
 * - Merge Maps operations into the previous operation (Map, Join) if possible.
 Logger.INSTANCE.setLoggingLevel(MonotoneAnalyzer.class, 4);
 * - Swap Maps with operations such as Distinct, Integral, Differential, etc.
 * - Merge Maps with subsequent MapIndex operators
 * - Merge consecutive apply operators
 */
public class OptimizeMaps extends CircuitCloneVisitor {
    final CircuitGraph graph;
    /** If true only optimize projections after joins */
    final boolean onlyProjections;

    public OptimizeMaps(IErrorReporter reporter, boolean onlyProjections, CircuitGraph graph) {
        super(reporter, false);
        this.graph = graph;
        this.onlyProjections = onlyProjections;
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        int inputFanout = this.graph.getFanout(operator.input());
        if (source.is(DBSPMapOperator.class)) {
            // mapindex(map) = mapindex
            DBSPClosureExpression expression = source.getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.errorReporter, expression);
            DBSPOperator result = new DBSPMapIndexOperator(
                    operator.getNode(), newFunction, operator.getOutputIndexedZSetType(), source.inputs.get(0));
            this.map(operator, result);
            return;
        } else if (source.is(DBSPMapIndexOperator.class)) {
            // mapindex(mapindex) = mapindex
            DBSPClosureExpression sourceFunction = source.getClosureFunction();
            DBSPClosureExpression thisFunction = operator.getClosureFunction();
            if (thisFunction.parameters.length != 1)
                throw new InternalCompilerError("Expected closure with 1 parameter", operator);
            DBSPExpression argument = new DBSPRawTupleExpression(
                    sourceFunction.body.field(0).borrow(),
                    sourceFunction.body.field(1).borrow());
            DBSPExpression apply = thisFunction.call(argument);
            DBSPClosureExpression newFunction = apply.closure(sourceFunction.parameters)
                    .reduce(errorReporter).to(DBSPClosureExpression.class);
            DBSPOperator result = new DBSPMapIndexOperator(
                    operator.getNode(), newFunction, operator.getOutputIndexedZSetType(), source.inputs.get(0));
            this.map(operator, result);
            return;
        } else if (inputFanout == 1) {
            Projection projection = new Projection(this.errorReporter);
            projection.apply(operator.getFunction());
            if (!this.onlyProjections || projection.isProjection) {
                if (source.is(DBSPJoinOperator.class)
                        || source.is(DBSPStreamJoinOperator.class)) {
                    DBSPOperator result = OptimizeProjectionVisitor.mapIndexAfterJoin(
                            this.errorReporter, source.to(DBSPJoinBaseOperator.class), operator);
                    this.map(operator, result);
                    return;
                } else if (source.is(DBSPJoinIndexOperator.class)
                        || source.is(DBSPStreamJoinIndexOperator.class)) {
                    DBSPOperator result = OptimizeProjectionVisitor.mapIndexAfterJoinIndex(
                            this.errorReporter, source.to(DBSPJoinBaseOperator.class), operator);
                    this.map(operator, result);
                    return;
                }
            }
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPMapIndexOperator.class)) {
            // deindex(mapindex) = nothing
            this.map(operator, source.inputs.get(0));
            return;
        }
        super.postorder(operator);
    }

    public void postorder(DBSPApplyOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        int inputFanout = this.graph.getFanout(operator.input());
        if (source.is(DBSPApplyOperator.class) && inputFanout == 1) {
            // apply(apply) = apply
            DBSPClosureExpression expression = source.getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.errorReporter, expression);
            DBSPOperator result = new DBSPApplyOperator(
                    operator.getNode(), newFunction, operator.outputType,
                    source.inputs.get(0), source.comment);
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        int inputFanout = this.graph.getFanout(operator.input());
        Projection projection = new Projection(this.errorReporter);
        projection.apply(operator.getFunction());
        if (source.is(DBSPJoinFilterMapOperator.class)) {
            if (!this.onlyProjections || projection.isProjection) {
                // map(joinfilter) = joinfilter
                DBSPJoinFilterMapOperator jfm = source.to(DBSPJoinFilterMapOperator.class);
                DBSPExpression newMap = operator.getFunction();
                if (jfm.map != null) {
                    newMap = operator.getClosureFunction()
                            .applyAfter(this.errorReporter, jfm.map.to(DBSPClosureExpression.class));
                }
                DBSPOperator result = new DBSPJoinFilterMapOperator(
                        jfm.getNode(), operator.getOutputZSetType(), jfm.getFunction(),
                        jfm.filter, newMap, operator.isMultiset, jfm.left(), jfm.right())
                        .copyAnnotations(operator).copyAnnotations(source);
                this.map(operator, result);
                return;
            }
        } else if ((source.is(DBSPStreamJoinOperator.class) ||
                source.is(DBSPAsofJoinOperator.class) ||
                source.is(DBSPJoinOperator.class)) &&
                // We have to look up the original operator input, not source
                inputFanout == 1) {
            if (!this.onlyProjections || projection.isProjection) {
                DBSPOperator result = OptimizeProjectionVisitor.mapAfterJoin(
                        this.errorReporter, source.to(DBSPJoinBaseOperator.class), operator);
                this.map(operator, result);
                return;
            }
        } else if (source.is(DBSPJoinIndexOperator.class) ||
                   source.is(DBSPStreamJoinIndexOperator.class) && inputFanout == 1) {
            if (!this.onlyProjections || projection.isProjection) {
                DBSPOperator result = OptimizeProjectionVisitor.mapAfterJoinIndex(
                        this.errorReporter, source.to(DBSPJoinBaseOperator.class), operator);
                this.map(operator, result);
                return;
            }
        } else if (source.is(DBSPMapOperator.class)) {
            DBSPClosureExpression expression = source.getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.errorReporter, expression);
            DBSPOperator result = source.withFunction(newFunction, operator.outputType);
            this.map(operator, result);
            return;
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
            return;
        }
        super.postorder(operator);
    }
}
