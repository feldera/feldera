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

        void standardMinMax(DBSPStreamAggregateOperator operator, MinMaxAggregate mm) {
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
            boolean isMin;
            switch (mm.operation) {
                case Min:
                    isMin = true;
                    break;
                case Max:
                    isMin = false;
                    break;
                case ArgMin, ArgMax:
                default:
                    throw new InternalCompilerError("Unexpected operation " + mm.operation);
            }
            DBSPMinMax.Aggregation aggregation = isMin ? DBSPMinMax.Aggregation.Min : DBSPMinMax.Aggregation.Max;
            if (isMin) {
                DBSPTypeTuple tuple = aggregatedValueType.to(DBSPTypeTuple.class);
                Utilities.enforce(tuple.size() == 1);
                DBSPType field = tuple.tupFields[0];
                if (field.mayBeNull) {
                    // For already nullable values use MinSome1
                    aggregation = DBSPMinMax.Aggregation.MinSome1;
                    aggregatedValueType = new DBSPTypeTuple(field.withMayBeNull(true));
                }
            }
            if (!aggregatedValueType.sameType(aggregationType)) {
                // This is important when computing the min of a ROW type;
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
                    MinMaxAggregate agg = aggregates.get(0);
                    if (agg.operation == MinMaxAggregate.Operation.Max ||
                            agg.operation == MinMaxAggregate.Operation.Min)
                        this.standardMinMax(operator, aggregates.get(0));
                    else
                        // TODO: optimize isolated ARG_MIN, ARG_MAX too
                        super.postorder(operator);
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
