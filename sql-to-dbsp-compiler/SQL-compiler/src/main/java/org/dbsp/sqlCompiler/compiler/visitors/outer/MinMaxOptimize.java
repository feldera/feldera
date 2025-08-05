package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/** Optimize the implementation of Min and Max aggregates.
 *
 * <p>Convert a single Min aggregate into a call to MinSome.
 *
 * <p>Convert a single Max aggregate into a call to Max.
 *
 * <p>For append-only streams, the following pattern:
 * mapIndex -> stream_aggregate(MinMaxAggregate)
 * is replaced with
 * mapIndex -> chain_aggregate.
 * The new mapIndex needs to index only the key and the aggregated field.
 * (The original mapIndex was keeping potentially more fields.) */
public class MinMaxOptimize extends Passes {
    final AppendOnly appendOnly;

    public MinMaxOptimize(DBSPCompiler compiler, DBSPVariablePath weightVar) {
        super("MinMaxOptimize", compiler);
        this.appendOnly = new AppendOnly(compiler);
        this.add(this.appendOnly);
        this.add(new ExpandMaxAsWindow(compiler, weightVar, this.appendOnly::isAppendOnly));
    }

    static class ExpandMaxAsWindow extends CircuitCloneVisitor {
        final Predicate<OutputPort> isAppendOnly;
        final DBSPVariablePath weightVar;

        public ExpandMaxAsWindow(DBSPCompiler compiler, DBSPVariablePath weightVar,
                                 Predicate<OutputPort> isAppendOnly) {
            super(compiler, false);
            this.isAppendOnly = isAppendOnly;
            this.weightVar = weightVar;
        }

        @Override
        public Token startVisit(IDBSPOuterNode circuit) {
            return super.startVisit(circuit);
        }

        void generalCase(DBSPStreamAggregateOperator operator, MinMaxAggregate mm) {
            OutputPort i = this.mapped(operator.input());

            if (!RemoveIdentityOperators.isIdentityFunction(mm.comparedValue)) {
                // compute the aggregated value in a prior projection
                var var = i.getOutputIndexedZSetType().getKVRefType().var();
                var projection = new DBSPRawTupleExpression(
                        DBSPTupleExpression.flatten(var.field(0).deref()),
                        new DBSPTupleExpression(mm.comparedValue.call(var.field(1))
                                .applyCloneIfNeeded()
                                .reduce(this.compiler))
                ).closure(var);
                DBSPMapIndexOperator reindex = new DBSPMapIndexOperator(operator.getRelNode(),
                        projection, i);
                this.addOperator(reindex);
                i = reindex.outputPort();
            }

            @Nullable DBSPClosureExpression postProcessing = null;
            // May need to apply a postProcessing step to convert the type to the one expected by Calcite
            DBSPType aggregatedValueType = i.getOutputIndexedZSetType().elementType;
            DBSPType aggregationType = operator.getOutputIndexedZSetType().elementType;
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
                        aggregation = DBSPMinMax.Aggregation.ArgMinSome;
                        DBSPType resultFieldType = fields.tupFields[1];
                        if (!resultFieldType.mayBeNull) {
                            // If ARG_MIN(a, b) has non-nullable b, we must convert the result
                            // of ArgMinSome to nullable.  note that ArgMinSome produces Tup1<B>.
                            DBSPVariablePath var = new DBSPTypeTuple(resultFieldType).ref().var();
                            postProcessing =
                                    new DBSPTupleExpression(
                                            var.deref()
                                                    .field(0)
                                                    .nullabilityCast(resultFieldType.withMayBeNull(true), false)
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

            DBSPMinMax aggregator = new DBSPMinMax(mm.getNode(), mm.type, postProcessing, aggregation);
            DBSPStreamAggregateOperator aggregate = new DBSPStreamAggregateOperator(operator.getRelNode(),
                    operator.getOutputIndexedZSetType(), aggregator, null, i);
            this.map(operator, aggregate);
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
            DBSPAggregateList aggregateList = operator.getAggregateList();
            if (!Linq.all(aggregateList.aggregates, a -> a.is(MinMaxAggregate.class))) {
                super.postorder(operator);
                return;
            }
            List<MinMaxAggregate> aggregates = Linq.map(aggregateList.aggregates, a -> a.to(MinMaxAggregate.class));
            if (!this.isAppendOnly.test(operator.input())) {
                if (aggregates.size() == 1) {
                    this.generalCase(operator, aggregates.get(0));
                } else {
                    super.postorder(operator);
                }
                return;
            }

            // Handle min/max/arg_min/arg_max for append-only streams.
            // Multiple of them are implemented using a single DBSPChainAggregate
            List<DBSPType> accumulatorTypes = new ArrayList<>();
            for (MinMaxAggregate mmAggregate: aggregates) {
                DBSPType accType = mmAggregate.getIncrementType();
                accumulatorTypes.add(accType);
            }

            DBSPTypeIndexedZSet inputType = i.getOutputIndexedZSetType();
            List<DBSPExpression> inits = new ArrayList<>();
            List<DBSPExpression> comparisons = new ArrayList<>();
            List<DBSPExpression> postProcessing = new ArrayList<>();

            DBSPType keyType = inputType.keyType;
            DBSPTypeTuple accumulatorTuple = new DBSPTypeTuple(accumulatorTypes);
            DBSPVariablePath acc = accumulatorTuple.var();
            DBSPVariablePath inputVar = inputType.elementType.ref().var();
            DBSPVariablePath postVar = new DBSPTypeRawTuple(
                    keyType.ref(),
                    accumulatorTuple.ref()).var();
            int ix = 0;
            boolean needsPost = false;
            for (MinMaxAggregate mmAggregate: aggregates) {
                DBSPType accType = mmAggregate.getIncrementType();
                DBSPClosureExpression postProcess = mmAggregate.postProcess;
                if (postProcess == null) {
                    postProcess = DBSPClosureExpression.id(accType);
                } else {
                    needsPost = true;
                }

                // The init function of the ChainAggregate takes the current data.
                // We use increment(zero, row).
                DBSPExpression zero = mmAggregate.increment.call(
                        mmAggregate.zero, inputVar, this.weightVar).reduce(this.compiler);
                inits.add(zero);
                accumulatorTypes.add(accType);

                DBSPExpression comparison = mmAggregate.increment.call(
                        acc.field(ix), inputVar, this.weightVar).reduce(this.compiler);
                comparisons.add(comparison);

                DBSPExpression post = postProcess.call(
                        postVar.field(1).deref().field(ix).applyCloneIfNeeded())
                        .reduce(this.compiler);
                postProcessing.add(post);
                ix++;
            }
            DBSPClosureExpression init = new DBSPTupleExpression(inits, false)
                    .closure(inputVar, this.weightVar);
            DBSPClosureExpression comparison = new DBSPTupleExpression(comparisons, false)
                    .closure(acc, inputVar, this.weightVar);

            DBSPSimpleOperator chain = new DBSPChainAggregateOperator(operator.getRelNode(),
                    init, comparison,
                    new DBSPTypeIndexedZSet(inputType.getNode(), inputType.keyType, acc.getType()), i);

            if (needsPost) {
                // If all operations are MIN or MAX no postprocessing is necessary.
                this.addOperator(chain);
                DBSPClosureExpression post = new DBSPRawTupleExpression(
                        postVar.field(0).deref(),
                        new DBSPTupleExpression(postProcessing, false))
                        .closure(postVar);
                chain = new DBSPMapIndexOperator(
                        operator.getRelNode(), post, operator.getOutputIndexedZSetType(), chain.outputPort());
            }

            this.map(operator, chain);
        }
    }
}
