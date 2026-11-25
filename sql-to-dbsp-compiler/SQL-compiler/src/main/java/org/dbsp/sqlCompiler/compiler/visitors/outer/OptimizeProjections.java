package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAtomicSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass.ProjectionRule;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.DetectShuffle;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Expensive;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Substitution;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FieldUseMap;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindUsedFields;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ParameterFieldUse;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.RewriteFields;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Maybe;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Optimizes projections (Map or MapIndex) following various other operators.
 * TODO: unify this with OptimizeProjectionsVisitor */
public class OptimizeProjections extends CircuitCloneWithGraphsVisitor {
    /** If true only optimize projections after joins */
    final boolean onlyProjections;
    final AnalyzedSet<DBSPOperator> operatorsAnalyzed;

    public OptimizeProjections(DBSPCompiler compiler, boolean onlyProjections,
                               CircuitGraphs graphs, AnalyzedSet<DBSPOperator> operatorsAnalyzed) {
        super(compiler, graphs);
        this.onlyProjections = onlyProjections;
        this.operatorsAnalyzed = operatorsAnalyzed;
    }

    private DBSPClosureExpression compose(DBSPClosureExpression before, DBSPClosureExpression after) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.composeProjection(this.compiler, before, after);
        return after.applyAfter(this.compiler, before, Maybe.YES);
    }

    @Nullable
    private DBSPClosureExpression tryCompose(DBSPClosureExpression before, DBSPClosureExpression after) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.tryComposeProjection(this.compiler, before, after);
        return after.shouldInlineComposition(this.compiler, before)
                ? after.applyAfter(this.compiler, before, Maybe.YES) : null;
    }

    @Nullable
    private DBSPClosureExpression composeIndexed(DBSPClosureExpression before, DBSPClosureExpression after) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.composeIndexedProjection(this.compiler, before, after, this.onlyProjections);
        boolean expensive = Expensive.isExpensive(this.compiler, before);
        Projection projection = new Projection(this.compiler);
        projection.apply(after);
        if (!expensive || (before.body.is(DBSPBaseTupleExpression.class) && projection.isProjection)) {
            var argument = new DBSPRawTupleExpression(before.body.field(0).borrow(), before.body.field(1).borrow());
            return after.call(argument).reduce(this.compiler).closure(before.parameters);
        }
        if (this.onlyProjections)
            return null;
        DBSPVariablePath pair = before.body.type.var();
        var borrowed = new DBSPRawTupleExpression(pair.field(0).borrow(), pair.field(1).borrow());
        var apply = new DBSPLetExpression(after.parameters[0].asVariable(), borrowed, after.body);
        return new DBSPLetExpression(pair, before.body, apply).closure(before.parameters);
    }

    private boolean isProjection(DBSPSimpleOperator map, boolean allowNestedAndCasts) {
        var function = map.getFunction();
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.isMapProjection(this.compiler, function, allowNestedAndCasts);
        var projection = new Projection(this.compiler, allowNestedAndCasts, allowNestedAndCasts);
        projection.apply(function);
        return projection.isProjection;
    }

    private boolean isIdentity(DBSPClosureExpression function) {
        return this.compiler.options.ioOptions.rustOptimizerPasses
                ? RustCircuitPass.isIdentityProjection(this.compiler, function)
                : RemoveIdentityOperators.isIdentityFunction(function);
    }

    @Nullable
    private DBSPFlatmap projectUnnest(DBSPFlatmap flatmap, DBSPClosureExpression reader) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.projectUnnest(this.compiler, flatmap, reader);
        var shuffle = DetectShuffle.analyze(this.compiler, reader);
        return shuffle == null ? null : flatmap.withShuffle(shuffle.after(flatmap.shuffle));
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

    private ProjectionRule producerRule(DBSPOperator source, boolean indexed, int fanout) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.projectionRule(this.compiler, source, indexed, fanout);

        if (source instanceof DBSPDifferentiateOperator || source instanceof DBSPDelayOperator
                || source instanceof DBSPNegateOperator || source instanceof DBSPNoopOperator)
            return ProjectionRule.PUSH_THROUGH;
        if (!indexed && (source instanceof DBSPSumOperator || source instanceof DBSPSubtractOperator
                || source instanceof DBSPAtomicSumOperator))
            return ProjectionRule.PROJECT_SUM;
        if (source instanceof DBSPAntiJoinOperator || source instanceof DBSPStreamAntiJoinOperator)
            return ProjectionRule.PROJECT_ANTI_JOIN;
        if (fanout != 1)
            return ProjectionRule.KEEP;
        if (source instanceof DBSPIntegrateOperator)
            return ProjectionRule.PUSH_THROUGH;
        if (source instanceof DBSPMapOperator)
            return ProjectionRule.COMPOSE_MAP;
        if (source instanceof DBSPJoinOperator || source instanceof DBSPLeftJoinOperator
                || source instanceof DBSPStreamJoinOperator || source instanceof DBSPStarJoinOperator)
            return ProjectionRule.PROJECT_JOIN;
        if (source instanceof DBSPJoinIndexOperator || source instanceof DBSPLeftJoinIndexOperator
                || source instanceof DBSPStreamJoinIndexOperator || source instanceof DBSPStarJoinIndexOperator)
            return ProjectionRule.PROJECT_INDEXED_JOIN;
        if (indexed) {
            if (source instanceof DBSPMapIndexOperator)
                return ProjectionRule.COMPOSE_INDEX;
            if (source instanceof DBSPStreamAggregateOperator)
                return ProjectionRule.TRIM_AGGREGATE;
            if (source instanceof DBSPIndexedTopKOperator)
                return ProjectionRule.TRIM_TOP_K;
        } else {
            if (source instanceof DBSPJoinFilterMapOperator)
                return ProjectionRule.PROJECT_FILTERED_JOIN;
            if (source instanceof DBSPStarJoinFilterMapOperator)
                return ProjectionRule.PROJECT_FILTERED_STAR;
            if (source instanceof DBSPAsofJoinOperator)
                return ProjectionRule.PROJECT_ASOF;
            if (source instanceof DBSPFlatMapOperator)
                return ProjectionRule.PROJECT_FLAT_MAP;
            if (source instanceof DBSPDeindexOperator)
                return ProjectionRule.PROJECT_DEINDEX;
            if (source instanceof DBSPAggregateZeroOperator)
                return ProjectionRule.PROJECT_AGGREGATE_ZERO;
        }
        return ProjectionRule.KEEP;
    }

    private boolean installProjectedAntijoin(DBSPBinaryOperator join, DBSPUnaryOperator reader, int width) {
        var result = RustCircuitPass.projectAntijoin(this.compiler, join, reader, width);
        var operators = result.operators();
        if (!result.replacesReader()) {
            operators.forEach(this::addOperator);
            return false;
        }
        operators.subList(0, operators.size() - 1).forEach(this::addOperator);
        this.map(reader, operators.getLast());
        return true;
    }

    private void pushThrough(DBSPUnaryOperator reader, DBSPSimpleOperator producer) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses) {
            var operators = RustCircuitPass.pushProjection(this.compiler, reader, producer);
            operators.subList(0, operators.size() - 1).forEach(this::addOperator);
            this.map(reader, operators.getLast());
            return;
        }
        var inputs = new ArrayList<OutputPort>();
        for (var input : producer.inputs) {
            var projection = reader.withInputs(List.of(input), true).to(DBSPSimpleOperator.class);
            this.addOperator(projection);
            inputs.add(projection.outputPort());
        }
        if (producer instanceof DBSPAggregateZeroOperator zero) {
            var emptyResult = reader.getClosureFunction().call(zero.getFunction().borrow()).reduce(this.compiler);
            this.map(reader, new DBSPAggregateZeroOperator(zero.getRelNode(), emptyResult, inputs.getFirst()));
        } else {
            this.map(reader, producer.withInputs(inputs, true).to(DBSPSimpleOperator.class));
        }
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (inputFanout != 1 || this.done(operator)) {
            super.postorder(operator);
            return;
        }
        OutputPort source = this.mapped(operator.input());
        var rule = this.producerRule(source.node(), true, inputFanout);
        int size = operator.outputType().getToplevelFieldCount();
        if (rule == ProjectionRule.COMPOSE_MAP && this.canMergeSource(source, size)) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("Map -> MapIndex")
                    .newline();
            // mapIndex(map) = mapIndex
            DBSPClosureExpression sourceFunction = source.simpleNode().getClosureFunction();
            DBSPClosureExpression afterFunction = operator.getClosureFunction();
            DBSPClosureExpression newFunction = this.tryCompose(sourceFunction, afterFunction);
            if (newFunction != null) {
                CalciteRelNode node = operator.getRelNode().after(source.node().getRelNode());
                DBSPSimpleOperator result = new DBSPMapIndexOperator(
                        node, newFunction, operator.getOutputIndexedZSetType(), source.node().inputs.get(0))
                        .copyAnnotations(operator).copyAnnotations(source.simpleNode());
                this.map(operator, result);
                return;
            }
        } else if (rule == ProjectionRule.COMPOSE_INDEX && this.canMergeSource(source, size)) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("MapIndex -> MapIndex")
                    .newline();
            // mapIndex(mapIndex) = mapIndex
            DBSPClosureExpression sourceFunction = source.simpleNode().getClosureFunction();
            DBSPClosureExpression thisFunction = operator.getClosureFunction();
            if (thisFunction.parameters.length != 1)
                throw new InternalCompilerError("Expected closure with 1 parameter", operator);

            var newFunction = this.composeIndexed(sourceFunction, thisFunction);
            if (newFunction == null) {
                super.postorder(operator);
                return;
            }
            CalciteRelNode node = operator.getRelNode().after(source.node().getRelNode());
            DBSPSimpleOperator result = new DBSPMapIndexOperator(
                    node, newFunction, operator.getOutputIndexedZSetType(), source.node().inputs.get(0))
                    .copyAnnotations(operator).copyAnnotations(source.simpleNode());
            this.map(operator, result);
            return;
        } else if (rule == ProjectionRule.PUSH_THROUGH) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> MapIndex")
                    .newline();
            this.pushThrough(operator, source.simpleNode());
            return;
        } else if (rule == ProjectionRule.TRIM_AGGREGATE) {
            DBSPStreamAggregateOperator aggregate = source.node().to(DBSPStreamAggregateOperator.class);
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> MapIndex")
                    .newline();
            if (!this.isProjection(operator, true) || aggregate.aggregateList == null) {
                super.postorder(operator);
                return;
            }
            boolean replaced = this.processAggregate(aggregate, operator);
            if (!replaced)
                super.postorder(operator);
            return;
        } else if (rule == ProjectionRule.TRIM_TOP_K) {
            DBSPIndexedTopKOperator topK = source.node().to(DBSPIndexedTopKOperator.class);
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> MapIndex")
                    .newline();
            if (!this.isProjection(operator, true)) {
                super.postorder(operator);
                return;
            }
            boolean replaced = this.processTopK(topK, operator);
            if (!replaced)
                super.postorder(operator);
            return;
        } else {
            // Projection analysis also validates bindings, even if the producer cannot absorb the reader.
            boolean projection = this.isProjection(operator, false);
            if (!this.onlyProjections || projection) {
                if (rule == ProjectionRule.PROJECT_JOIN) {
                    Logger.INSTANCE.belowLevel(this, 2)
                            .appendSupplier(() -> source.simpleNode().operation + " -> MapIndex")
                            .newline();
                    DBSPSimpleOperator result = OptimizeProjectionVisitor.mapIndexAfterJoin(
                            this.compiler(), source.simpleNode(), operator);
                    this.map(operator, result);
                    return;
                } else if (rule == ProjectionRule.PROJECT_INDEXED_JOIN) {
                    Logger.INSTANCE.belowLevel(this, 2)
                            .appendSupplier(() -> source.simpleNode().operation + " -> MapIndex")
                            .newline();
                    DBSPSimpleOperator result = OptimizeProjectionVisitor.mapIndexAfterJoinIndex(
                            this.compiler(), source.simpleNode(), operator);
                    this.map(operator, result);
                    return;
                }
            }
            if (rule == ProjectionRule.PROJECT_ANTI_JOIN && projection) {
                Logger.INSTANCE.belowLevel(this, 2)
                        .appendSupplier(() -> source.simpleNode().operation + " -> MapIndex")
                        .newline();
                DBSPBinaryOperator join = source.node().to(DBSPBinaryOperator.class);
                if (this.compiler.options.ioOptions.rustOptimizerPasses) {
                    if (!this.installProjectedAntijoin(join, operator, size))
                        super.postorder(operator);
                    return;
                }
                OutputPort left = join.left();
                OutputPort right = join.right();

                DBSPClosureExpression proj = operator.getClosureFunction();
                // We must preserve keys unchanged, but we can project away any values
                Pair<DBSPClosureExpression, DBSPClosureExpression> split = this.splitClosure(proj);

                OutputPort leftPort = left;
                if (!this.isIdentity(split.left)) {
                    // Identical index operators on both sides
                    DBSPSimpleOperator leftIndex = new DBSPMapIndexOperator(operator.getRelNode(),
                            split.left, left).addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
                    this.addOperator(leftIndex);
                    leftPort = leftIndex.outputPort();
                }

                // On the right of the antijoin we can drop all value fields, but we only do this if
                // the right input of the antijoin does not have other outputs.
                OutputPort rightPort = right;
                DBSPClosureExpression closure = keysOnly(right.getOutputIndexedZSetType());
                if (!this.isIdentity(closure)) {
                    DBSPSimpleOperator rightIndex = new DBSPMapIndexOperator(operator.getRelNode(),
                            closure, right).addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
                    this.addOperator(rightIndex);
                    rightPort = rightIndex.outputPort();
                }

                DBSPSimpleOperator newJoin = join
                        .withInputs(Linq.list(leftPort, rightPort), false)
                        .to(DBSPSimpleOperator.class);
                if (newJoin.outputType.sameType(join.outputType)) {
                    super.postorder(operator);
                    return;
                }

                // Now project the keys after the join
                if (this.isIdentity(split.right)) {
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
     *                 The assumption is that C only depends on A, but not on B.
     */
    Pair<DBSPClosureExpression, DBSPClosureExpression> splitClosure(DBSPClosureExpression closure) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.splitAntijoinProjection(this.compiler, closure);
        Utilities.enforce(closure.parameters.length == 1);
        final DBSPParameter param = closure.parameters[0];
        final DBSPTypeRawTuple paramType = param.getType().to(DBSPTypeRawTuple.class);

        final DBSPRawTupleExpression tuple = closure.body.to(DBSPRawTupleExpression.class);
        Utilities.enforce(tuple.fields != null);
        Utilities.enforce(tuple.fields.length == 2);
        DBSPVariablePath var0 = param.asVariable();
        // Check that C does not depend on B, i.e., var0.1
        InnerVisitor dependsOnB = new InnerVisitor(this.compiler) {
            @Override
            public void postorder(DBSPFieldExpression expression) {
                if (expression.fieldNo == 1) {
                    if (expression.expression.is(DBSPVariablePath.class)) {
                        DBSPVariablePath var = expression.expression.to(DBSPVariablePath.class);
                        Utilities.enforce(!var0.variable.equals(var.variable),
                                () -> "splitClosure cannot decompose closure " + closure);
                    }
                }
            }
        };
        dependsOnB.apply(tuple.fields[0]);

        final DBSPExpression v00 = var0.field(0).deref();
        final DBSPExpression flat;
        if (v00.getType().is(DBSPTypeTupleBase.class))
            flat = new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var0.field(0).deref()), false);
        else
            flat = v00.applyCloneIfNeeded();
        DBSPClosureExpression first =
                new DBSPRawTupleExpression(flat, tuple.fields[1].applyCloneIfNeeded()).closure(param);

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
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.antijoinProjection(this.compiler, paramType, null);
        DBSPVariablePath var = paramType.getKVRefType().var();
        return new DBSPRawTupleExpression(
                        new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var.field(0).deref()), false),
                        new DBSPTupleExpression()).closure(var);
    }

    private DBSPClosureExpression projectAntijoinValue(DBSPTypeIndexedZSet input, DBSPClosureExpression projection) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.antijoinProjection(this.compiler, input, projection);
        DBSPVariablePath pair = input.getKVRefType().var();
        return new DBSPRawTupleExpression(
                ExpressionCompiler.expandTuple(projection.getNode(), pair.field(0).deref()),
                projection.call(pair).reduce(this.compiler)).closure(pair);
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
        protected boolean rewritesTypes() {
            return false;
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
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("Apply -> Apply")
                    .newline();
            DBSPApplyOperator apply = source.node().to(DBSPApplyOperator.class);
            // apply(apply) = apply
            DBSPClosureExpression expression = apply.getClosureFunction();
            DBSPClosureExpression newFunction = this.compose(expression, operator.getClosureFunction());
            DBSPSimpleOperator result = new DBSPApplyOperator(
                    operator.getRelNode(), newFunction, operator.outputType,
                    apply.inputs.get(0), apply.comment);
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }

    /** Drop aggregate components the following map never reads. */
    boolean processAggregate(DBSPStreamAggregateOperator aggregate, DBSPMapIndexOperator map) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return this.trimProjectedOutput(aggregate, map);
        var aggregates = Objects.requireNonNull(aggregate.aggregateList);
        var output = aggregate.getOutputIndexedZSetType();
        var reader = map.getClosureFunction();
        var projection = this.aggregateProjection(aggregates, output, reader);
        if (projection == null)
            return false;

        var projectedType = new DBSPTypeIndexedZSet(output.getNode(), output.keyType, projection.left.getEmptySetResultType());
        var narrowed = new DBSPStreamAggregateOperator(aggregate.getRelNode(), projectedType,
                null, projection.left, aggregate.input());
        this.addOperator(narrowed);
        var filler = new DBSPMapIndexOperator(map.getRelNode(), projection.right, narrowed.outputPort());
        this.addOperator(filler);
        this.map(map, map.withInputs(Linq.list(filler.outputPort()), false).to(DBSPSimpleOperator.class));
        return true;
    }

    @Nullable
    private Pair<DBSPAggregateList, DBSPClosureExpression> aggregateProjection(
            DBSPAggregateList aggregates, DBSPTypeIndexedZSet output, DBSPClosureExpression reader) {
        var uses = new FindUsedFields(this.compiler).findUsedFields(reader);
        var valueUse = uses.get(reader.parameters[0]).field(1);
        if (!valueUse.hasUnusedFields(1))
            return null;
        var retained = valueUse.deref().getUsedFields();
        if (retained.size() == aggregates.size())
            return null;

        var list = new DBSPAggregateList(aggregates.getNode(), aggregates.rowVar, Linq.map(retained, aggregates.aggregates::get));
        var pair = new DBSPTypeRawTuple(output.keyType.ref(), list.getEmptySetResultType().ref()).var();
        // Recreate the reader's input with the dropped aggregates' empty-set results.
        var fields = new ArrayList<DBSPExpression>();
        int offset = 0;
        for (int i = 0; i < aggregates.size(); i++) {
            if (retained.contains(i))
                fields.add(pair.field(1).deref().field(offset++));
            else
                fields.add(aggregates.aggregates.get(i).getEmptySetResult());
        }
        var filler = new DBSPRawTupleExpression(
                new DBSPTupleExpression(DBSPTypeTuple.flatten(pair.field(0).deref()), false),
                new DBSPTupleExpression(fields, false)).closure(pair);
        return Pair.of(list, filler);
    }

    /** Process a TopK followed by a MapIndex operator; return 'true' if the operators
     * required changes, false if they are unchanged.  When the
     * operators require change, they are inserted in the circuit and map is replaced. */
    boolean processTopK(DBSPIndexedTopKOperator topK, DBSPMapIndexOperator map) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return this.trimProjectedOutput(topK, map);
        var functions = this.topKClosures(topK, map);
        if (functions == null)
            return false;
        DBSPSimpleOperator newTopK = new DBSPIndexedTopKOperator(topK.getRelNode(), topK.numbering,
                topK.getFunction(), topK.limit, topK.equalityComparator, functions.left, topK.input())
                .copyAnnotations(topK).to(DBSPSimpleOperator.class);
        this.addOperator(newTopK);

        DBSPSimpleOperator result = new DBSPMapIndexOperator(map.getRelNode(), functions.right, newTopK.outputPort())
                .copyAnnotations(map).to(DBSPSimpleOperator.class);
        this.map(map, result);
        return true;
    }

    private boolean trimProjectedOutput(DBSPUnaryOperator producer, DBSPMapIndexOperator reader) {
        var operators = RustCircuitPass.trimProjectedOutput(this.compiler, producer, reader);
        if (operators.isEmpty())
            return false;
        operators.subList(0, operators.size() - 1).forEach(this::addOperator);
        this.map(reader, operators.getLast());
        return true;
    }

    @Nullable
    private Pair<DBSPClosureExpression, DBSPClosureExpression> topKClosures(
            DBSPIndexedTopKOperator topK, DBSPMapIndexOperator map) {
        DBSPClosureExpression function = map.getClosureFunction();
        DBSPClosureExpression topKClosure = topK.outputProducer
                .ensureTree(this.compiler).to(DBSPClosureExpression.class);
        DBSPTupleExpression produced = topKClosure.body.as(DBSPTupleExpression.class);
        if (produced == null || produced.fields == null || produced.getType().mayBeNull)
            return null;

        ParameterFieldUse uses = new FindUsedFields(this.compiler).findUsedFields(function);
        FieldUseMap useMap = uses.get(function.parameters[0]);
        useMap.setUsed(0);
        if (!useMap.hasUnusedFields(2))
            return null;

        List<Integer> keptOutputs = useMap.field(1).deref().getUsedFields();
        List<DBSPExpression> kept = Linq.map(keptOutputs, i -> produced.fields[i].deepCopy());
        DBSPClosureExpression newProducer = new DBSPTupleExpression(kept, false).closure(topKClosure.parameters);
        RewriteFields rewriter = uses.getFieldRewriter(this.compiler, 2);
        DBSPClosureExpression post = rewriter.rewriteClosure(function);
        return Pair.of(newProducer, post);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        if (this.done(operator)) {
            super.postorder(operator);
            return;
        }
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());

        var rule = this.producerRule(source.node(), false, inputFanout);
        int size = operator.outputType().getToplevelFieldCount();
        if (rule == ProjectionRule.PROJECT_FILTERED_JOIN || rule == ProjectionRule.PROJECT_FILTERED_STAR) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> Map")
                    .newline();
            if (this.isProjection(operator, false) || !this.onlyProjections) {
                DBSPSimpleOperator result;
                if (this.compiler.options.ioOptions.rustOptimizerPasses) {
                    result = RustCircuitPass.projectFilteredJoin(this.compiler, source.simpleNode(), operator);
                } else {
                    var reader = operator.getClosureFunction();
                    if (source.node() instanceof DBSPJoinFilterMapOperator join) {
                        var map = join.map == null ? reader : this.compose(join.map, reader);
                        result = new DBSPJoinFilterMapOperator(join.getRelNode(), operator.getOutputZSetType(),
                                join.getFunction(), join.filter, map, join.left(), join.right(), join.balanced);
                    } else {
                        var join = source.node().to(DBSPStarJoinFilterMapOperator.class);
                        var map = join.map == null ? reader : this.compose(join.map, reader);
                        result = new DBSPStarJoinFilterMapOperator(join.getRelNode(), operator.getOutputZSetType(),
                                join.getClosureFunction(), join.filter, map, join.inputs);
                    }
                    result.copyAnnotations(source.node());
                }
                this.map(operator, result);
                return;
            }
        } else if (rule == ProjectionRule.PROJECT_JOIN) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> Map")
                    .newline();
            if (this.isProjection(operator, false) || !this.onlyProjections) {
                DBSPSimpleOperator result = OptimizeProjectionVisitor.mapAfterJoin(
                        this.compiler(), source.simpleNode(), operator);
                this.map(operator, result);
                return;
            }
        } else if (rule == ProjectionRule.PROJECT_ASOF) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> Map")
                    .newline();
            if (this.isProjection(operator, false)) {
                // Only combine with pure projections
                DBSPSimpleOperator result = OptimizeProjectionVisitor.mapAfterJoin(
                        this.compiler(), source.simpleNode(), operator);
                this.map(operator, result);
                return;
            }
        } else if (rule == ProjectionRule.PROJECT_INDEXED_JOIN) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> Map")
                    .newline();
            if (this.isProjection(operator, false) || !this.onlyProjections) {
                DBSPSimpleOperator result = OptimizeProjectionVisitor.mapAfterJoinIndex(
                        this.compiler(), source.simpleNode(), operator);
                this.map(operator, result);
                return;
            }
        } else if (rule == ProjectionRule.PROJECT_FLAT_MAP && operator.getFunction().is(DBSPClosureExpression.class)) {
            DBSPFlatmap sourceFlatmap = source.simpleNode().getFunction().as(DBSPFlatmap.class);
            var newFunction = sourceFlatmap == null ? null : this.projectUnnest(sourceFlatmap, operator.getClosureFunction());
            if (newFunction != null) {
                Logger.INSTANCE.belowLevel(this, 2)
                        .appendSupplier(() -> source.simpleNode().operation + " -> Map")
                        .newline();
                DBSPSimpleOperator result = source.simpleNode()
                        .withFunction(newFunction, operator.outputType)
                        .to(DBSPSimpleOperator.class);
                this.map(operator, result);
                return;
            }
        } else if (rule == ProjectionRule.COMPOSE_MAP &&
                this.canMergeSource(source, size) &&
                source.simpleNode().getFunction().is(DBSPClosureExpression.class)) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("Map -> Map")
                    .newline();
            DBSPClosureExpression sourceFunction = source.simpleNode().getClosureFunction();
            DBSPClosureExpression afterFunction = operator.getClosureFunction();
            DBSPClosureExpression newFunction = this.tryCompose(sourceFunction, afterFunction);
            if (newFunction != null) {
                DBSPSimpleOperator result = source.simpleNode()
                        .withFunction(newFunction, operator.outputType)
                        .to(DBSPSimpleOperator.class);
                this.map(operator, result);
                return;
            }
        } else if (rule == ProjectionRule.PROJECT_DEINDEX) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> Map")
                    .newline();
            DBSPClosureExpression expression = source.simpleNode().getClosureFunction();
            DBSPClosureExpression newFunction = this.compose(expression, operator.getClosureFunction());
            OutputPort input = source.simpleNode().inputs.get(0);
            DBSPSimpleOperator result =
                    new DBSPMapOperator(source.node().getRelNode(), newFunction, input);
            this.map(operator, result);
            return;
        } else if (rule == ProjectionRule.PUSH_THROUGH || rule == ProjectionRule.PROJECT_SUM) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> Map")
                    .newline();
            if (rule == ProjectionRule.PROJECT_SUM) {
                if (!this.isProjection(operator, true)) {
                    // swapping arbitrary maps with sum is not sound
                    // since it may apply operations like div by 0 to tuples that may never appear
                    super.postorder(operator);
                    return;
                }
            }

            this.pushThrough(operator, source.simpleNode());
            return;
        } else if (rule == ProjectionRule.PROJECT_AGGREGATE_ZERO) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> Map")
                    .newline();
            if (!this.isProjection(operator, true)) {
                super.postorder(operator);
                return;
            }
            this.pushThrough(operator, source.simpleNode());
            return;
        }

        if (rule == ProjectionRule.PROJECT_ANTI_JOIN) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> source.simpleNode().operation + " -> Map")
                    .newline();
            DBSPBinaryOperator join = source.node().to(DBSPBinaryOperator.class);
            if (this.isProjection(operator, false) && inputFanout == 1) {
                if (this.compiler.options.ioOptions.rustOptimizerPasses) {
                    if (!this.installProjectedAntijoin(join, operator, size))
                        super.postorder(operator);
                    return;
                }
                OutputPort left = join.left();
                OutputPort right = join.right();

                DBSPClosureExpression proj = operator.getClosureFunction();
                // We must preserve keys unchanged, but we can project away any values
                var computeValue = this.projectAntijoinValue(left.getOutputIndexedZSetType(), proj);

                OutputPort leftPort = left;
                if (!this.isIdentity(computeValue)) {
                    // Identical index operators on both sides
                    DBSPSimpleOperator leftIndex = new DBSPMapIndexOperator(operator.getRelNode(),
                            computeValue, left).addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
                    this.addOperator(leftIndex);
                    leftPort = leftIndex.outputPort();
                }

                // On the right of the antijoin we can drop all value fields, but we only do this if
                // the right input of the antijoin does not have other outputs.
                OutputPort rightPort = right;
                DBSPClosureExpression closure = keysOnly(join.right().getOutputIndexedZSetType());
                if (!this.isIdentity(closure)) {
                    DBSPSimpleOperator rightIndex = new DBSPMapIndexOperator(operator.getRelNode(),
                            closure, right).addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
                    this.addOperator(rightIndex);
                    rightPort = rightIndex.outputPort();
                }

                DBSPSimpleOperator newJoin = join.withInputs(Linq.list(leftPort, rightPort), false)
                        .to(DBSPSimpleOperator.class);
                if (newJoin.outputType.sameType(join.outputType)) {
                    super.postorder(operator);
                    return;
                }

                // Now project the keys after the join
                if (newJoin != join)
                    this.addOperator(newJoin);
                DBSPSimpleOperator result = new DBSPDeindexOperator(
                        operator.getRelNode(), newJoin.getFunctionNode(), newJoin.outputPort());
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }
}
