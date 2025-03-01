package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.util.Maybe;
import org.dbsp.util.Shuffle;

/** Optimizes patterns containing projections.
 * - constant followed by projection
 * - flatmap followed by projection
 * - join followed by projection
 * - join followed by mapindex projection
 * - indexjoin followed by mapindex projection
 * - indexjoin followed by map projection
 * Projections are map operations that have a function with a very simple
 * structure.  The function is analyzed using the 'Projection' visitor. */
public class OptimizeProjectionVisitor extends CircuitCloneWithGraphsVisitor {
    public OptimizeProjectionVisitor(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        OutputPort source = this.mapped(operator.input());
        DBSPExpression function = operator.getFunction();
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        Projection projection = new Projection(this.compiler, true);
        projection.apply(function);
        if (projection.isProjection) {
            if (source.node().is(DBSPConstantOperator.class)) {
                DBSPExpression newConstant = projection.applyAfter(
                        source.node().to(DBSPConstantOperator.class).getFunction().to(DBSPZSetExpression.class));
                DBSPSimpleOperator result = source.simpleNode().withFunction(newConstant, operator.outputType);
                this.map(operator, result);
                return;
            } else if (source.node().is(DBSPFlatMapOperator.class)) {
                DBSPFlatmap sourceFunction = source.simpleNode().getFunction().as(DBSPFlatmap.class);
                if (sourceFunction != null && projection.isShuffle()) {
                    DBSPTypeFunction previousFunctionType = sourceFunction.getType().to(DBSPTypeFunction.class);
                    DBSPTypeFunction newFunctionType = new DBSPTypeFunction(
                            operator.getOutputZSetElementType(), previousFunctionType.parameterTypes);

                    Shuffle shuffle = projection.getShuffle().after(sourceFunction.shuffle);
                    DBSPExpression newFunction = new DBSPFlatmap(
                            operator.getRelNode(), newFunctionType, sourceFunction.inputElementType,
                            sourceFunction.collectionExpression, sourceFunction.leftInputIndexes,
                            sourceFunction.rightProjections, sourceFunction.ordinalityIndexType, shuffle);
                    DBSPSimpleOperator result = source.simpleNode().withFunction(newFunction, operator.outputType);
                    this.map(operator, result);
                    return;
                }
            } else if (source.node().is(DBSPJoinOperator.class)
                    || source.node().is(DBSPStreamJoinOperator.class)
                    || source.node().is(DBSPAsofJoinOperator.class)) {
                if (inputFanout == 1 && projection.isShuffle()) {
                    // We only do this if the source is a projection, because then the join function
                    // will still have a simple shape.  Subsequent analyses may care about this.
                    DBSPSimpleOperator result = mapAfterJoin(
                            this.compiler, source.node().to(DBSPJoinBaseOperator.class), operator);
                    this.map(operator, result);
                    return;
                }
            } else if (source.node().is(DBSPJoinIndexOperator.class) ||
                    source.node().is(DBSPStreamJoinIndexOperator.class)) {
                if (inputFanout == 1 && projection.isShuffle()) {
                    // We only do this if the source is a projection, because then the join function
                    // will still have a simple shape.  Subsequent analyses may care about this.
                    DBSPSimpleOperator result = mapAfterJoinIndex(
                            this.compiler, source.node().to(DBSPJoinBaseOperator.class), operator);
                    this.map(operator, result);
                    return;
                }
            }
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        OutputPort source = this.mapped(operator.input());
        DBSPExpression function = operator.getFunction();
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        Projection projection = new Projection(this.compiler, true);
        projection.apply(function);
        if (inputFanout == 1 && projection.isProjection && projection.isShuffle()) {
            if (source.node().is(DBSPJoinOperator.class)
                    || source.node().is(DBSPStreamJoinOperator.class)
                    || source.node().is(DBSPJoinIndexOperator.class)
                    || source.node().is(DBSPStreamJoinIndexOperator.class)) {
                DBSPSimpleOperator result = mapIndexAfterJoin(
                        this.compiler, source.node().to(DBSPJoinBaseOperator.class), operator);
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }

    static DBSPJoinBaseOperator mapAfterJoin(
            DBSPCompiler reporter, DBSPJoinBaseOperator source, DBSPMapOperator operator) {
        DBSPClosureExpression joinFunction = source.getClosureFunction();
        DBSPExpression function = operator.getFunction();
        DBSPExpression newFunction = function.to(DBSPClosureExpression.class)
                .applyAfter(reporter, joinFunction, Maybe.YES);
        return source.withFunction(newFunction, operator.outputType).to(DBSPJoinBaseOperator.class);
    }

    static DBSPJoinBaseOperator mapAfterJoinIndex(
            DBSPCompiler reporter, DBSPJoinBaseOperator source, DBSPMapOperator operator) {
        DBSPJoinBaseOperator sourceJoin = source.to(DBSPJoinBaseOperator.class);
        DBSPClosureExpression joinFunction = source.getClosureFunction();
        DBSPClosureExpression function = operator.getClosureFunction();
        if (function.parameters.length != 1)
            throw new InternalCompilerError("Expected closure with 1 parameter", operator);
        DBSPExpression argument = new DBSPRawTupleExpression(
                joinFunction.body.field(0).borrow(),
                joinFunction.body.field(1).borrow());
        DBSPExpression apply = function.call(argument);
        DBSPClosureExpression newFunction = apply.closure(joinFunction.parameters)
                .reduce(reporter).to(DBSPClosureExpression.class);
        if (source.is(DBSPJoinIndexOperator.class)) {
            return new DBSPJoinOperator(source.getRelNode(), operator.getOutputZSetType(),
                    newFunction, operator.isMultiset, sourceJoin.left(), sourceJoin.right());
        } else {
            assert source.is(DBSPStreamJoinIndexOperator.class);
            return new DBSPStreamJoinOperator(source.getRelNode(), operator.getOutputZSetType(),
                    newFunction, operator.isMultiset, sourceJoin.left(), sourceJoin.right());
        }
    }

    static DBSPJoinBaseOperator mapIndexAfterJoin(
            DBSPCompiler reporter, DBSPJoinBaseOperator source, DBSPMapIndexOperator operator) {
        DBSPExpression function = operator.getFunction();
        DBSPClosureExpression joinFunction = source.getClosureFunction();
        DBSPExpression newFunction = function.to(DBSPClosureExpression.class)
                .applyAfter(reporter, joinFunction, Maybe.YES);
        CalciteRelNode node = operator.getRelNode().after(source.getRelNode());
        if (source.is(DBSPJoinOperator.class)) {
            return new DBSPJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, operator.isMultiset, source.left(), source.right());
        } else if (source.is(DBSPStreamJoinOperator.class)) {
            return new DBSPStreamJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, operator.isMultiset, source.left(), source.right());
        } else {
            return source.withFunction(newFunction, operator.outputType).to(DBSPJoinBaseOperator.class);
        }
    }

    static DBSPJoinBaseOperator mapIndexAfterJoinIndex(
            DBSPCompiler reporter, DBSPJoinBaseOperator source, DBSPMapIndexOperator operator) {
        DBSPClosureExpression joinFunction = source.getClosureFunction();
        DBSPClosureExpression function = operator.getClosureFunction();
        if (function.parameters.length != 1)
            throw new InternalCompilerError("Expected closure with 1 parameter", operator);
        DBSPExpression argument = new DBSPRawTupleExpression(
                joinFunction.body.field(0).borrow(),
                joinFunction.body.field(1).borrow());
        DBSPExpression apply = function.call(argument);
        DBSPClosureExpression newFunction = apply.closure(joinFunction.parameters)
                .reduce(reporter).to(DBSPClosureExpression.class);
        CalciteRelNode node = operator.getRelNode().after(source.getRelNode());
        if (source.is(DBSPJoinIndexOperator.class)) {
            return new DBSPJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, operator.isMultiset, source.left(), source.right());
        } else {
            assert source.is(DBSPStreamJoinIndexOperator.class);
            return new DBSPStreamJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, operator.isMultiset, source.left(), source.right());
        }
    }
}
