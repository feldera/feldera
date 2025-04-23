package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
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
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Substitution;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Maybe;

import java.util.ArrayList;
import java.util.List;

/** Optimizes patterns containing Map operators. */
public class OptimizeMaps extends CircuitCloneWithGraphsVisitor {
    /** If true only optimize projections after joins */
    final boolean onlyProjections;
    final AnalyzedSet<DBSPOperator> operatorsAnalyzed;

    public OptimizeMaps(DBSPCompiler compiler, boolean onlyProjections,
                        CircuitGraphs graphs, AnalyzedSet<DBSPOperator> operatorsAnalyzed) {
        super(compiler, graphs, false);
        this.onlyProjections = onlyProjections;
        this.operatorsAnalyzed = operatorsAnalyzed;
    }

    boolean done(DBSPOperator operator) {
        return this.operatorsAnalyzed.done(operator);
    }

    boolean canMergeSource(OutputPort source, int size) {
        if (!this.onlyProjections)
            return true;
        IsProjection proj = source.node().annotations.first(IsProjection.class);
        if (proj == null)
            return true;
        return proj.outputSize > size;
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (inputFanout != 1 || this.done(operator)) {
            super.postorder(operator);
            return;
        }
        OutputPort source = this.mapped(operator.input());
        int size = operator.outputType().getToplevelFieldCount();
        if (source.node().is(DBSPMapOperator.class) && this.canMergeSource(source, size)) {
            // mapIndex(map) = mapIndex
            DBSPClosureExpression expression = source.simpleNode().getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.compiler(), expression, Maybe.MAYBE);
            CalciteRelNode node = operator.getRelNode().after(source.node().getRelNode());
            DBSPSimpleOperator result = new DBSPMapIndexOperator(
                    node, newFunction, operator.getOutputIndexedZSetType(), source.node().inputs.get(0));
            this.map(operator, result);
            return;
        } else if (source.node().is(DBSPMapIndexOperator.class) && this.canMergeSource(source, size)) {
            // mapIndex(mapIndex) = mapIndex
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
            CalciteRelNode node = operator.getRelNode().after(source.node().getRelNode());
            DBSPSimpleOperator result = new DBSPMapIndexOperator(
                    node, newFunction, operator.getOutputIndexedZSetType(), source.node().inputs.get(0));
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
            if (projection.isProjection && operator.hasAnnotation(a -> a.is(IsProjection.class)) &&
                    (source.node().is(DBSPAntiJoinOperator.class)
                    || source.node().is(DBSPStreamAntiJoinOperator.class))) {
                DBSPBinaryOperator join = source.node().to(DBSPBinaryOperator.class);
                OutputPort left = join.left();
                OutputPort right = join.right();

                DBSPClosureExpression proj = operator.getClosureFunction();
                // We must preserve keys unchanged, but we can project away any values
                Pair<DBSPClosureExpression, DBSPClosureExpression> split = this.splitClosure(proj);

                OutputPort leftPort = left;
                if (!RemoveIdentityOperators.isIdentityFunction(split.left)) {
                    // Identical index operators on both sides
                    DBSPSimpleOperator leftIndex = new DBSPMapIndexOperator(operator.getRelNode(),
                            split.left, left).addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
                    this.addOperator(leftIndex);
                    leftPort = leftIndex.outputPort();
                }

                // On the right of the antijoin we can drop all value fields, but we only do this if
                // the right input of the antijoin does not have other outputs.
                OutputPort rightPort = right;
                DBSPClosureExpression closure = keysOnly(join.right().getOutputIndexedZSetType());
                if (!RemoveIdentityOperators.isIdentityFunction(closure)) {
                    DBSPSimpleOperator rightIndex = new DBSPMapIndexOperator(operator.getRelNode(),
                            closure, right).addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
                    this.addOperator(rightIndex);
                    rightPort = rightIndex.outputPort();
                }

                DBSPSimpleOperator newJoin = join.withInputs(Linq.list(leftPort, rightPort), false);

                // Now project the keys after the join
                if (RemoveIdentityOperators.isIdentityFunction(split.right)) {
                    this.map(operator, newJoin);
                } else {
                    if (newJoin != join)
                        this.addOperator(newJoin);
                    DBSPSimpleOperator result = new DBSPMapIndexOperator(operator.getRelNode(),
                            split.right, newJoin.outputPort());
                    this.map(operator, result);
                }
                return;
            }
        }
        super.postorder(operator);
    }

    /** Split a closure that produces elements of an IndexedZSet into two closures
     * that compose to the same result.
     * @param closure  A closure with signature (A, B) -> (C, D)
     * @return         A pair of closures.
     *                 The first one has signature (A, B) -> (A, D).  First component is identity.
     *                 The second one has signature (A, D) -> (C, D).  Second component is identity.
     */
    Pair<DBSPClosureExpression, DBSPClosureExpression> splitClosure(DBSPClosureExpression closure) {
        assert closure.parameters.length == 1;
        DBSPParameter param = closure.parameters[0];
        DBSPTypeRawTuple paramType = param.getType().to(DBSPTypeRawTuple.class);

        DBSPRawTupleExpression tuple = closure.body.to(DBSPRawTupleExpression.class);
        assert tuple.fields != null;
        DBSPVariablePath var0 = param.asVariable();
        DBSPClosureExpression first =
                new DBSPRawTupleExpression(
                        new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var0.field(0).deref()), false),
                        tuple.fields[1].applyCloneIfNeeded()).closure(param);

        // Use same name as parameter
        DBSPVariablePath var1 = new DBSPVariablePath(param.name, new DBSPTypeRawTuple(
                paramType.tupFields[0],
                tuple.fields[1].getType().ref()));
        // The variable has a different type from the parameter.
        // References to the parameter are not free variables in 'tuple'.
        ReplaceFreeVariable replace = new ReplaceFreeVariable(this.compiler, var1);
        DBSPExpression replaced = replace.apply(tuple.fields[0]).to(DBSPExpression.class);
        DBSPClosureExpression second = new DBSPRawTupleExpression(
                replaced.applyCloneIfNeeded(),
                new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var1.field(1).deref()), false))
                .closure(var1);

        return new Pair<>(first, second);
    }

    DBSPClosureExpression keysOnly(DBSPTypeIndexedZSet paramType) {
        DBSPVariablePath var = paramType.getKVRefType().var();
        return new DBSPRawTupleExpression(
                        new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var.field(0).deref()), false),
                        new DBSPTupleExpression()).closure(var);
    }

    /** Replace all references to the (only) free variable with another variable */
    static class ReplaceFreeVariable extends InnerRewriteVisitor {
        final Substitution<DBSPVariablePath, DBSPVariablePath> newParam;
        final ResolveReferences resolver;
        final DBSPVariablePath replacement;

        protected ReplaceFreeVariable(DBSPCompiler compiler, DBSPVariablePath replacement) {
            super(compiler, false);
            this.newParam = new Substitution<>();
            this.replacement = replacement;
            this.resolver = new ResolveReferences(compiler, true);
        }

        @Override
        public VisitDecision preorder(DBSPVariablePath var) {
            IDBSPDeclaration declaration = this.resolver.reference.get(var);
            if (declaration == null) {
                this.map(var, this.replacement);
                return VisitDecision.STOP;
            }
            return super.preorder(var);
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            this.resolver.apply(node);
            super.startVisit(node);
        }
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
        if (source.node().is(DBSPApplyOperator.class) &&
                inputFanout == 1 && !this.done(operator)) {
            DBSPApplyOperator apply = source.node().to(DBSPApplyOperator.class);
            // apply(apply) = apply
            DBSPClosureExpression expression = apply.getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfter(this.compiler(), expression, Maybe.YES);
            DBSPSimpleOperator result = new DBSPApplyOperator(
                    operator.getRelNode(), newFunction, operator.outputType,
                    apply.inputs.get(0), apply.comment);
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        if (this.done(operator)) {
            super.postorder(operator);
            return;
        }
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());

        int size = operator.outputType().getToplevelFieldCount();
        if (source.node().is(DBSPJoinFilterMapOperator.class) &&
                inputFanout == 1) {
            Projection projection = new Projection(this.compiler());
            projection.apply(operator.getFunction());
            if (!this.onlyProjections || projection.isProjection) {
                // map(joinFilter) = joinFilter
                DBSPJoinFilterMapOperator jfm = source.node().to(DBSPJoinFilterMapOperator.class);
                DBSPExpression newMap = operator.getFunction();
                if (jfm.map != null) {
                    newMap = operator.getClosureFunction()
                            .applyAfter(this.compiler(), jfm.map.to(DBSPClosureExpression.class), Maybe.YES);
                }
                DBSPSimpleOperator result = new DBSPJoinFilterMapOperator(
                        jfm.getRelNode(), operator.getOutputZSetType(), jfm.getFunction(),
                        jfm.filter, newMap, operator.isMultiset, jfm.left(), jfm.right())
                        .copyAnnotations(operator).copyAnnotations(source.node()).to(DBSPSimpleOperator.class);
                this.map(operator, result);
                return;
            }
        } else if ((source.node().is(DBSPStreamJoinOperator.class) ||
                source.node().is(DBSPAsofJoinOperator.class) ||
                source.node().is(DBSPJoinOperator.class)) &&
                inputFanout == 1) {
            Projection projection = new Projection(this.compiler());
            projection.apply(operator.getFunction());
            if (!this.onlyProjections || projection.isProjection) {
                DBSPSimpleOperator result = OptimizeProjectionVisitor.mapAfterJoin(
                        this.compiler(), source.node().to(DBSPJoinBaseOperator.class), operator);
                this.map(operator, result);
                return;
            }
        } else if (source.node().is(DBSPJoinIndexOperator.class) ||
                source.node().is(DBSPStreamJoinIndexOperator.class) &&
                        inputFanout == 1) {
            Projection projection = new Projection(this.compiler());
            projection.apply(operator.getFunction());
            if (!this.onlyProjections || projection.isProjection) {
                DBSPSimpleOperator result = OptimizeProjectionVisitor.mapAfterJoinIndex(
                        this.compiler(), source.node().to(DBSPJoinBaseOperator.class), operator);
                this.map(operator, result);
                return;
            }
        } else if (source.node().is(DBSPMapOperator.class) && inputFanout == 1 &&
                this.canMergeSource(source, size)) {
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
                (source.node().is(DBSPSumOperator.class)) ||
                (source.node().is(DBSPSubtractOperator.class)) ||
                source.node().is(DBSPNoopOperator.class)) {
            if (source.node().is(DBSPSumOperator.class) || source.node().is(DBSPSubtractOperator.class)) {
                Projection projection = new Projection(this.compiler());
                projection.apply(operator.getFunction());
                if (!projection.isProjection) {
                    // swapping arbitrary maps with sum is not sound
                    // since it may apply operations like div by 0 to tuples that may never appear
                    super.postorder(operator);
                    return;
                }
            }

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
