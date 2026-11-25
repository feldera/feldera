package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.IJoin;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.DetectShuffle;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Maybe;
import org.dbsp.util.Utilities;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Optimizes patterns containing projections.
 * - constant followed by projection
 * - flatmap followed by projection
 * - join followed by projection
 * - join followed by mapIndex projection
 * - indexJoin followed by mapIndex projection
 * - indexJoin followed by map projection
 * Projections are map operations that have a function with a very simple
 * structure.  The function is analyzed using the 'Projection' visitor. */
public class OptimizeProjectionVisitor extends CircuitCloneWithGraphsVisitor {
    public OptimizeProjectionVisitor(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs);
    }

    /** Fold a projection using the functions on its mapped input. A preceding
     * projection in the same pass can change which operator produces that input. */
    public DBSPCircuit applyNative(DBSPCircuit circuit) {
        return new CircuitCloneWithGraphsVisitor(this.compiler, this.graphs) {
            @Override
            public void replace(DBSPSimpleOperator reader) {
                if (reader instanceof DBSPMapOperator || reader instanceof DBSPMapIndexOperator) {
                    var input = reader.inputs.get(0);
                    var mapped = this.mapped(input).node();
                    // Resolving the reader can report a diagnostic even when its input cannot absorb it.
                    if (RustCircuitPass.isOutputProjection(this.compiler, reader) &&
                            mapped instanceof DBSPSimpleOperator source && source.function != null &&
                            (source instanceof DBSPConstantOperator || source instanceof DBSPFlatMapOperator || source instanceof IJoin)) {
                        boolean single = this.getGraph().getFanout(input.node()) == 1;
                        var result = RustCircuitPass.foldOutputProjection(this.compiler, source, reader, single);
                        if (result != null) {
                            this.map(reader, result);
                            return;
                        }
                    }
                }
                super.replace(reader);
            }
        }.apply(circuit);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        OutputPort source = this.mapped(operator.input());
        DBSPExpression function = operator.getFunction();
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        Projection projection = new Projection(this.compiler, true, true);
        projection.apply(function);
        if (projection.isProjection) {
            if (source.node().is(DBSPConstantOperator.class)) {
                DBSPZSetExpression before = source.node().to(DBSPConstantOperator.class)
                        .getFunction()
                        .to(DBSPZSetExpression.class);
                Objects.requireNonNull(projection.expression);

                Map<DBSPExpression, Long> result1 = new LinkedHashMap<>();
                DBSPType elementType = projection.expression.getResultType();
                for (Map.Entry<DBSPExpression, Long> entry: before.data.entrySet()) {
                    DBSPExpression row = entry.getKey();
                    DBSPExpression simplified = projection.expression
                            .call(row.borrow())
                            .reduce(this.compiler);
                    result1.put(simplified, entry.getValue());
                }
                DBSPExpression newConstant = new DBSPZSetExpression(result1, elementType);
                DBSPSimpleOperator result = source.simpleNode()
                        .withFunction(newConstant, operator.outputType)
                        .to(DBSPSimpleOperator.class);
                this.map(operator, result);
                return;
            } else if (source.node().is(DBSPFlatMapOperator.class)) {
                DBSPFlatmap sourceFunction = source.simpleNode().getFunction().as(DBSPFlatmap.class);
                var shuffle = DetectShuffle.analyze(this.compiler, function.to(DBSPClosureExpression.class));
                if (sourceFunction != null && shuffle != null) {
                    shuffle = shuffle.after(sourceFunction.shuffle);
                    DBSPExpression newFunction = sourceFunction.withShuffle(shuffle);
                    DBSPSimpleOperator result = source.simpleNode()
                            .withFunction(newFunction, operator.outputType)
                            .to(DBSPSimpleOperator.class);
                    this.map(operator, result);
                    return;
                }
            } else if (source.node().is(DBSPJoinOperator.class)
                    || source.node().is(DBSPLeftJoinOperator.class)
                    || source.node().is(DBSPStreamJoinOperator.class)
                    || source.node().is(DBSPAsofJoinOperator.class)
                    || source.node().is(DBSPStarJoinOperator.class)) {
                if (inputFanout == 1 && projection.isOnlyFieldAccesses()) {
                    // We only do this if the source is a projection, because then the join function
                    // will still have a simple shape.  Subsequent analyses may care about this.
                    DBSPSimpleOperator result = mapAfterJoin(
                            this.compiler, source.simpleNode(), operator);
                    this.map(operator, result);
                    return;
                }
            } else if (source.node().is(DBSPJoinIndexOperator.class)
                    || source.node().is(DBSPLeftJoinIndexOperator.class)
                    || source.node().is(DBSPStreamJoinIndexOperator.class)
                    || source.node().is(DBSPStarJoinIndexOperator.class)) {
                if (inputFanout == 1 && projection.isOnlyFieldAccesses()) {
                    // We only do this if the source is a projection, because then the join function
                    // will still have a simple shape.  Subsequent analyses may care about this.
                    DBSPSimpleOperator result = mapAfterJoinIndex(
                            this.compiler, source.simpleNode(), operator);
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
        Projection projection = new Projection(this.compiler, true, false);
        projection.apply(function);
        if (inputFanout == 1 && projection.isProjection && projection.isOnlyFieldAccesses()) {
            if (source.node().is(DBSPJoinOperator.class)
                    || source.node().is(DBSPStreamJoinOperator.class)
                    || source.node().is(DBSPLeftJoinOperator.class)
                    || source.node().is(DBSPStarJoinOperator.class)) {
                DBSPSimpleOperator result = mapIndexAfterJoin(
                        this.compiler, source.simpleNode(), operator);
                this.map(operator, result);
                return;
            } else if (source.node().is(DBSPJoinIndexOperator.class)
                    || source.node().is(DBSPLeftJoinIndexOperator.class)
                    || source.node().is(DBSPStreamJoinIndexOperator.class)
                    || source.node().is(DBSPStarJoinIndexOperator.class)) {
                DBSPSimpleOperator result = mapIndexAfterJoinIndex(
                        this.compiler, source.simpleNode(), operator);
                this.map(operator, result);
                return;
            } else if (source.node().is(DBSPConstantOperator.class)) {
                DBSPExpression c = source.node().to(DBSPConstantOperator.class).getFunction();
                // Currently we can only simplify if the source is an empty ZSet.
                // In that case the output is also empty.
                boolean isEmpty = false;
                if (c.is(DBSPZSetExpression.class)) {
                    isEmpty = c.to(DBSPZSetExpression.class).isEmpty();
                } else if (c.is(DBSPIndexedZSetExpression.class)) {
                    isEmpty = c.to(DBSPIndexedZSetExpression.class).isEmpty();
                }
                if (isEmpty) {
                    DBSPIndexedZSetExpression empty = new DBSPIndexedZSetExpression(
                            c.getNode(), operator.getOutputIndexedZSetType());
                    DBSPConstantOperator result = new DBSPConstantOperator(
                            operator.getRelNode(), empty);
                    this.map(operator, result);
                    return;
                }
            }
        }
        super.postorder(operator);
    }

    private static DBSPClosureExpression composeJoinOutput(
            DBSPCompiler compiler, DBSPSimpleOperator source, DBSPSimpleOperator projection) {
        var before = source.getClosureFunction();
        var after = projection.getClosureFunction();
        if (!source.outputType.is(DBSPTypeIndexedZSet.class))
            return after.applyAfter(compiler, before, Maybe.YES);
        if (after.parameters.length != 1)
            throw new InternalCompilerError("Expected closure with 1 parameter", projection);
        var borrowed = new DBSPRawTupleExpression(before.body.field(0).borrow(), before.body.field(1).borrow());
        return after.call(borrowed).reduce(compiler).closure(before.parameters);
    }

    static DBSPSimpleOperator mapAfterJoin(
            DBSPCompiler compiler, DBSPSimpleOperator source, DBSPMapOperator operator) {
        if (compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.projectJoinOutput(compiler, source, operator);
        var newFunction = composeJoinOutput(compiler, source, operator);
        return source.withFunction(newFunction, operator.outputType)
                .copyAnnotations(source)
                .to(DBSPSimpleOperator.class);
    }

    static DBSPSimpleOperator mapAfterJoinIndex(
            DBSPCompiler compiler, DBSPSimpleOperator source, DBSPMapOperator operator) {
        if (compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.projectJoinOutput(compiler, source, operator);
        var newFunction = composeJoinOutput(compiler, source, operator);
        final DBSPJoinBaseOperator join = source.as(DBSPJoinBaseOperator.class);
        if (source.is(DBSPJoinIndexOperator.class)) {
            Utilities.enforce(join != null);
            return new DBSPJoinOperator(source.getRelNode(), operator.getOutputZSetType(),
                    newFunction, join.left(), join.right(), join.balanced)
                    .copyAnnotations(source);
        } else if (source.is(DBSPLeftJoinIndexOperator.class)) {
            Utilities.enforce(join != null);
            return new DBSPLeftJoinOperator(source.getRelNode(), operator.getOutputZSetType(),
                    newFunction, join.left(), join.right(), join.balanced)
                    .copyAnnotations(source);
        } else if (source.is(DBSPStreamJoinIndexOperator.class)) {
            Utilities.enforce(join != null);
            return new DBSPStreamJoinOperator(source.getRelNode(), operator.getOutputZSetType(),
                    newFunction, join.left(), join.right(), join.balanced)
                    .copyAnnotations(source);
        } else if (source.is(DBSPStarJoinIndexOperator.class)) {
            return new DBSPStarJoinOperator(source.getRelNode(), operator.getOutputZSetType(), newFunction,
                    source.inputs)
                    .copyAnnotations(source);
        }
        throw new InternalCompilerError("Unexpected join operator " + source);
    }

    static DBSPSimpleOperator mapIndexAfterJoin(
            DBSPCompiler compiler, DBSPSimpleOperator source, DBSPMapIndexOperator operator) {
        if (compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.projectJoinOutput(compiler, source, operator);
        var newFunction = composeJoinOutput(compiler, source, operator);
        CalciteRelNode node = operator.getRelNode().after(source.getRelNode());
        DBSPJoinBaseOperator join = source.as(DBSPJoinBaseOperator.class);
        
        if (source.is(DBSPJoinOperator.class)) {
            Utilities.enforce(join != null);
            return new DBSPJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, join.left(), join.right(), join.balanced)
                    .copyAnnotations(source);
        } else if (source.is(DBSPStreamJoinOperator.class)) {
            Utilities.enforce(join != null);
            return new DBSPStreamJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, join.left(), join.right(), join.balanced)
                    .copyAnnotations(source);
        } else if (source.is(DBSPLeftJoinOperator.class)) {
            Utilities.enforce(join != null);
            return new DBSPLeftJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, join.left(), join.right(), join.balanced)
                    .copyAnnotations(source);
        } else if (source.is(DBSPStarJoinOperator.class)) {
            return new DBSPStarJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, source.inputs)
                    .copyAnnotations(source);
        } else {
            return source.withFunction(newFunction, operator.outputType).to(DBSPSimpleOperator.class);
        }
    }

    static DBSPSimpleOperator mapIndexAfterJoinIndex(
            DBSPCompiler compiler, DBSPSimpleOperator source, DBSPMapIndexOperator operator) {
        if (compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.projectJoinOutput(compiler, source, operator);
        var newFunction = composeJoinOutput(compiler, source, operator);
        CalciteRelNode node = operator.getRelNode().after(source.getRelNode());

        DBSPJoinBaseOperator join = source.as(DBSPJoinBaseOperator.class);
        if (source.is(DBSPJoinIndexOperator.class)) {
            Utilities.enforce(join != null);
            return new DBSPJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, join.left(), join.right(), join.balanced)
                    .copyAnnotations(join);
        } else if (source.is(DBSPLeftJoinIndexOperator.class)) {
            Utilities.enforce(join != null);
            return new DBSPLeftJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, join.left(), join.right(), join.balanced)
                    .copyAnnotations(join);
        } else if (source.is(DBSPStarJoinIndexOperator.class)) {
            return new DBSPStarJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, source.inputs)
                    .copyAnnotations(source);
        } else if (source.is(DBSPStreamJoinIndexOperator.class)) {
            Utilities.enforce(join != null);
            return new DBSPStreamJoinIndexOperator(node, operator.getOutputIndexedZSetType(),
                    newFunction, join.left(), join.right(), join.balanced)
                    .copyAnnotations(source);
        }
        throw new InternalCompilerError("Unexpected join " + source);
    }
}
