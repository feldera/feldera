package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.IdShuffle;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Expand {@link DBSPStreamAggregateOperator} into multiple operators,
 * depending on whether the sources are append-only. */
public class ExpandAggregates extends Passes {
    final AppendOnly appendOnly;

    public ExpandAggregates(DBSPCompiler compiler, DBSPVariablePath weightVar) {
        super("OptimizeAggregates", compiler);
        this.appendOnly = new AppendOnly(compiler);
        this.add(this.appendOnly);
        this.add(new ExpandStreamAggregates(compiler, weightVar, this.appendOnly::isAppendOnly));
    }

    record GroupedAggregates(List<DBSPAggregateList> groups, Shuffle shuffle) {}

    static GroupedAggregates reorderAggregates(DBSPAggregateList aggregates, boolean appendOnly) {
        if (aggregates.size() == 1) {
            return new GroupedAggregates(Linq.list(aggregates), new IdShuffle(1));
        }
        // Make a new list for each IAggregate which is not compatible with any other previous aggregate
        List<List<Integer>> indexes = new ArrayList<>();
        List<List<IAggregate>> groups = new ArrayList<>();

        for (int i = 0; i < aggregates.size(); i++) {
            IAggregate aggregate = aggregates.aggregates.get(i);
            boolean added = false;
            for (int groupIndex = 0; groupIndex < indexes.size(); groupIndex++) {
                List<Integer> ix = indexes.get(groupIndex);
                List<IAggregate> g = groups.get(groupIndex);
                IAggregate inGroup = Utilities.last(g);
                if (inGroup.compatible(aggregate, appendOnly)) {
                    // This implicitly relies on the transitivity of compatibility
                    ix.add(i);
                    g.add(aggregate);
                    added = true;
                    break;
                }
            }
            if (!added) {
                // Create a new group.
                List<Integer> ix = new ArrayList<>();
                List<IAggregate> g = new ArrayList<>();
                indexes.add(ix);
                groups.add(g);
                ix.add(i);
                g.add(aggregate);
            }
        }

        List<Integer> shuffleIndexes = new ArrayList<>(aggregates.size());
        for (var group: indexes)
            shuffleIndexes.addAll(group);
        Utilities.enforce(shuffleIndexes.size() == aggregates.size());
        Shuffle shuffle = new ExplicitShuffle(aggregates.size(), shuffleIndexes);
        List<DBSPAggregateList> lists = Linq.map(groups,
                g -> new DBSPAggregateList(aggregates.getNode(), aggregates.rowVar, g));
        return new GroupedAggregates(lists, shuffle);
    }

    /** Splits the aggregates inside a DBSPStreamAggregate operator into multiple aggregate operators
     * and combines the results. */
    static class ExpandStreamAggregates extends CircuitCloneVisitor {
        final Predicate<OutputPort> isAppendOnly;
        final DBSPVariablePath weightVar;

        public ExpandStreamAggregates(DBSPCompiler compiler, DBSPVariablePath weightVar,
                                      Predicate<OutputPort> isAppendOnly) {
            super(compiler, false);
            this.isAppendOnly = isAppendOnly;
            this.weightVar = weightVar;
        }

        /** Implement one Min/Max/ArgMin/ArgMax aggregate for sources which are not append-only.
         * @param aggregationType Type of result produced by mm. */
        DBSPSimpleOperator implementOneMinMax(
                CalciteRelNode node, OutputPort input, MinMaxAggregate mm, DBSPType aggregationType) {
            if (!RemoveIdentityOperators.isIdentityFunction(mm.comparedValue)) {
                // compute mm.comparedValue in a prior MapIndex projection
                var var = input.getOutputIndexedZSetType().getKVRefType().var();
                var projection = new DBSPRawTupleExpression(
                        DBSPTupleExpression.flatten(var.field(0).deref()),
                        new DBSPTupleExpression(mm.comparedValue.call(var.field(1))
                                .applyCloneIfNeeded()
                                .reduce(this.compiler))
                ).closure(var);
                DBSPMapIndexOperator reindex = new DBSPMapIndexOperator(node, projection, input);
                this.addOperator(reindex);
                input = reindex.outputPort();
            }

            @Nullable DBSPClosureExpression postProcessing = null;
            // May need to apply a postProcessing step to convert the type to the one expected by Calcite
            DBSPType aggregatedValueType = input.getOutputIndexedZSetType().elementType;
            DBSPMinMax.Aggregation aggregation;
            switch (mm.operation) {
                case Max: {
                    aggregation = DBSPMinMax.Aggregation.Max;
                    break;
                }
                case Min: {
                    DBSPTypeTuple tuple = aggregatedValueType.to(DBSPTypeTuple.class);
                    Utilities.enforce(tuple.size() == 1);
                    DBSPType field = tuple.tupFields[0];
                    aggregation = DBSPMinMax.Aggregation.Min;
                    if (field.mayBeNull) {
                        // For already nullable values use MinSome1
                        aggregation = DBSPMinMax.Aggregation.MinSome1;
                        aggregatedValueType = new DBSPTypeTuple(field.withMayBeNull(true));
                    }
                    break;
                }
                case ArgMax: {
                    aggregation = DBSPMinMax.Aggregation.Max;
                    // ArgMax is implemented as Max; use postprocessing to extract correct field
                    postProcessing = this.refinePostProcessing(mm, aggregatedValueType, aggregationType);
                    break;
                }
                case ArgMin: {
                    // aggregatedValue is Tup1<(compared, result)>
                    DBSPTypeTuple tuple = aggregatedValueType.to(DBSPTypeTuple.class);
                    Utilities.enforce(tuple.size() == 1);
                    DBSPTypeRawTuple fields = tuple.tupFields[0].to(DBSPTypeRawTuple.class);
                    Utilities.enforce(fields.tupFields.length == 2);
                    aggregation = DBSPMinMax.Aggregation.Min;
                    if (fields.tupFields[0].mayBeNull) {
                        // If the compared value is nullable, we cannot use Min, we need to use ArgMinSome, which
                        // ignores nulls in the first component
                        aggregation = DBSPMinMax.Aggregation.ArgMinSome;
                        DBSPTypeTuple resultType = aggregationType.to(DBSPTypeTuple.class);
                        Utilities.enforce(resultType.size() == 1);
                        if (resultType.tupFields[0].mayBeNull) {
                            // If ARG_MIN(a, b) has non-nullable b, we may need convert the result
                            // of ArgMinSome to nullable.  Note that ArgMinSome produces Tup1<B>.
                            DBSPVariablePath var = new DBSPTypeTuple(fields.tupFields[1]).ref().var();
                            postProcessing = new DBSPTupleExpression(
                                    var.deref().field(0)
                                            .cast(mm.getNode(), resultType.tupFields[0], false)
                                            .applyCloneIfNeeded()
                            ).closure(var);
                        }
                    } else {
                        // ArgMin implemented as Min; use postProcessing to extract correct field
                        postProcessing = this.refinePostProcessing(mm, aggregatedValueType, aggregationType);
                    }
                    break;
                }
                default:
                    throw new InternalCompilerError("Unexpected aggregation operation " + mm.operation);
            }

            if (!aggregatedValueType.sameType(aggregationType) &&
                    (mm.operation == MinMaxAggregate.Operation.Max || mm.operation == MinMaxAggregate.Operation.Min)) {
                // This is important when computing the min/max of a ROW type;
                // this may insert casts to match the type expected
                postProcessing = aggregatedValueType.caster(aggregationType, false)
                        .reduce(this.compiler)
                        .to(DBSPClosureExpression.class);
            }

            DBSPType functionType = new DBSPTypeFunction(mm.type, input.getOutputIndexedZSetType().keyType);
            DBSPMinMax aggregator = new DBSPMinMax(mm.getNode(), functionType, postProcessing, aggregation);
            DBSPTypeIndexedZSet ix = new DBSPTypeIndexedZSet(
                    aggregationType.getNode(), input.getOutputIndexedZSetType().keyType, aggregationType);
            return new DBSPStreamAggregateOperator(node, ix, aggregator, null, input);
        }

        private DBSPClosureExpression refinePostProcessing(
                MinMaxAggregate mm, DBSPType aggregatedValueType, DBSPType aggregationType) {
            // mm.postProcess takes cast(comparedValue) as an argument,
            // but the Max supplies Tup1<comparedValue> as argument.
            @Nullable DBSPClosureExpression postProcessing;
            Utilities.enforce(mm.postProcess != null);
            Utilities.enforce(mm.postProcess.parameters.length == 1);
            DBSPTypeRawTuple postProcessTuple = mm.postProcess.parameters[0].getType().to(DBSPTypeRawTuple.class);
            // Value produced by Max
            DBSPVariablePath var = aggregatedValueType.ref().var();
            DBSPExpression comparedValue = var.deref().field(0);
            DBSPTypeTuple aggregationTuple = aggregationType.to(DBSPTypeTuple.class);  // Tup1<T>; extract T
            DBSPExpression pair = new DBSPRawTupleExpression(
                    comparedValue.field(0).nullabilityCast(postProcessTuple.tupFields[0], false),
                    comparedValue.field(1).cast(mm.getNode(), aggregationTuple.tupFields[0], false));
            DBSPExpression body = mm.postProcess.call(pair).reduce(this.compiler);
            // The result of postProcess must also be wrapped in a Tup1.
            DBSPExpression tup1 = new DBSPTupleExpression(body);
            postProcessing = tup1.closure(var);
            return postProcessing;
        }

        @Override
        public void postorder(DBSPStreamAggregateOperator operator) {
            if (operator.aggregateList == null) {
                super.postorder(operator);
                return;
            }
            OutputPort i = this.mapped(operator.input());
            DBSPTypeIndexedZSet inputType = i.getOutputIndexedZSetType();
            boolean appendOnly = this.isAppendOnly.test(operator.input());

            GroupedAggregates ga = reorderAggregates(operator.getAggregateList(), appendOnly);
            CalciteRelNode node = operator.getRelNode();
            List<DBSPSimpleOperator> aggregates = Linq.map(ga.groups,
                    g -> this.implementAggregateGroup(node, inputType.keyType, i, g, appendOnly));
            DBSPSimpleOperator result = CalciteToDBSPCompiler.combineAggregateList(node, inputType.keyType, aggregates, this::addOperator);
            if (!ga.shuffle.isIdentityPermutation()) {
                DBSPTypeIndexedZSet ix = result.getOutputIndexedZSetType();
                DBSPVariablePath vReorder = ix.getKVRefType().var();
                DBSPTupleExpression key = DBSPTupleExpression.flatten(vReorder.field(0).deref());
                Shuffle inverse = ga.shuffle.invert();
                DBSPBaseTupleExpression value =
                        DBSPTupleExpression.flatten(vReorder.field(1).deref())
                                .shuffle(inverse);
                DBSPClosureExpression closure = new DBSPRawTupleExpression(key, value).closure(vReorder);
                result = new DBSPMapIndexOperator(node, closure, result.outputPort());
                this.addOperator(result);
            }
            this.map(operator, result, false);
        }

        /** Implement a list of compatible aggregates using a single operator */
        DBSPSimpleOperator implementAggregateGroup(
                CalciteRelNode node, DBSPType keyType, OutputPort input, DBSPAggregateList group, boolean appendOnly) {
            Utilities.enforce(!group.isEmpty());
            IAggregate first = group.aggregates.get(0);
            DBSPType valueType = group.getEmptySetResultType();
            DBSPTypeIndexedZSet outputType = TypeCompiler.makeIndexedZSet(keyType, valueType);
            DBSPSimpleOperator result;
            if (first.is(NonLinearAggregate.class) && !first.is(MinMaxAggregate.class)) {
                result = new DBSPStreamAggregateOperator(node, outputType, null, group, input);
            } else {
                if (appendOnly) {
                    result = this.implementChain(node, keyType, input, group);
                } else {
                    if (first.is(LinearAggregate.class)) {
                        // incremental-only operator
                        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, input);
                        this.addOperator(diff);
                        LinearAggregate linear = group.asLinear(this.compiler());
                        DBSPSimpleOperator aggOp = new DBSPAggregateLinearPostprocessOperator(
                                node, outputType, linear.map, linear.postProcess, diff.outputPort());
                        this.addOperator(aggOp);
                        result = new DBSPIntegrateOperator(node, aggOp.outputPort());
                    } else {
                        Utilities.enforce(first.is(MinMaxAggregate.class));
                        Utilities.enforce(group.size() == 1);
                        result = this.implementOneMinMax(node, input, first.to(MinMaxAggregate.class), valueType);
                    }
                }
            }
            this.addOperator(result);
            return result;
        }

        /** Implement multiple aggregates as a {@link DBSPChainAggregateOperator} */
        DBSPSimpleOperator implementChain(
                CalciteRelNode node, DBSPType keyType, OutputPort input, DBSPAggregateList group) {
            DBSPTypeIndexedZSet inputType = input.getOutputIndexedZSetType();

            List<DBSPType> accumulatorTypes = new ArrayList<>();
            for (IAggregate aggregate: group.aggregates) {
                DBSPType accType = aggregate.getAccumulatorType();
                accumulatorTypes.add(accType);
            }

            List<DBSPExpression> inits = new ArrayList<>();
            List<DBSPExpression> comparisons = new ArrayList<>();
            List<DBSPExpression> postProcessing = new ArrayList<>();

            DBSPTypeTuple accumulatorTuple = new DBSPTypeTuple(accumulatorTypes);
            DBSPVariablePath acc = accumulatorTuple.var();
            DBSPVariablePath inputVar = inputType.elementType.ref().var();
            DBSPVariablePath postVar = new DBSPTypeRawTuple(
                    keyType.ref(),
                    accumulatorTuple.ref()).var();
            int ix = 0;
            boolean needsPost = false;
            for (IAggregate aggregate: group.aggregates) {
                DBSPType accType = aggregate.getAccumulatorType();
                DBSPExpression init;
                DBSPExpression comparison;
                DBSPExpression post;
                DBSPExpression postField = postVar.field(1).deref().field(ix).applyCloneIfNeeded();

                if (aggregate.is(MinMaxAggregate.class)) {
                    MinMaxAggregate mmAggregate = aggregate.to(MinMaxAggregate.class);
                    DBSPClosureExpression postProcess = mmAggregate.postProcess;
                    if (postProcess == null) {
                        postProcess = DBSPClosureExpression.id(accType);
                    } else {
                        needsPost = true;
                    }

                    // The init function of the ChainAggregate takes the current data.
                    // We use increment(zero, row).
                    init = mmAggregate.increment.call(
                            mmAggregate.zero, inputVar, this.weightVar).reduce(this.compiler);
                    comparison = mmAggregate.increment.call(
                            acc.field(ix), inputVar, this.weightVar).reduce(this.compiler);
                    post = postProcess.call(postField).reduce(this.compiler);
                } else {
                    LinearAggregate linear = aggregate.to(LinearAggregate.class);
                    init = linear.map.call(inputVar).reduce(this.compiler)
                            .mulByWeight(this.weightVar);
                    comparison = linear.map.call(inputVar).reduce(this.compiler)
                            .mulByWeight(this.weightVar).add(acc.field(ix));
                    needsPost = true;
                    post = linear.postProcess.call(postField).reduce(this.compiler);
                }
                inits.add(init);
                comparisons.add(comparison);
                postProcessing.add(post);
                ix++;
            }
            DBSPClosureExpression init = new DBSPTupleExpression(inits, false)
                    .closure(inputVar, this.weightVar);
            DBSPClosureExpression comparison = new DBSPTupleExpression(comparisons, false)
                    .closure(acc, inputVar, this.weightVar);

            DBSPSimpleOperator chain = new DBSPChainAggregateOperator(node,
                    init, comparison, TypeCompiler.makeIndexedZSet(keyType, acc.getType()), input);

            if (needsPost) {
                // If all operations are MIN or MAX no postprocessing is necessary.
                this.addOperator(chain);
                DBSPClosureExpression post = new DBSPRawTupleExpression(
                        postVar.field(0).deref(),
                        new DBSPTupleExpression(postProcessing, false))
                        .closure(postVar);
                chain = new DBSPMapIndexOperator(node, post, chain.outputPort());
            }

            return chain;
        }
    }
}
