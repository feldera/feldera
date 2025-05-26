package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalAggregateExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
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

            if (!RemoveIdentityOperators.isIdentityFunction(mm.aggregatedValue)) {
                // compute the aggregated value in a prior projection
                var var = i.getOutputIndexedZSetType().getKVRefType().var();
                var projection = new DBSPRawTupleExpression(
                        DBSPTupleExpression.flatten(var.field(0).deref()),
                        new DBSPTupleExpression(mm.aggregatedValue.call(var.field(1))
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
            DBSPMinMax.Aggregation aggregation = mm.isMin ? DBSPMinMax.Aggregation.Min : DBSPMinMax.Aggregation.Max;
            if (mm.isMin) {
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
            if (operator.aggregate == null) {
                super.postorder(operator);
                return;
            }
            OutputPort i = this.mapped(operator.input());
            DBSPAggregateList aggregateList = operator.getAggregate();
            if (!Linq.all(aggregateList.aggregates, a -> a.is(MinMaxAggregate.class))) {
                super.postorder(operator);
                return;
            }
            List<MinMaxAggregate> aggregates = Linq.map(aggregateList.aggregates, a -> a.to(MinMaxAggregate.class));
            if (!this.isAppendOnly.test(operator.input())) {
                if (aggregates.size() == 1) {
                    this.standardMinMax(operator, aggregates.get(0));
                } else {
                    super.postorder(operator);
                }
                return;
            }

            // Handle min/max for append-only streams, implemented using a ChainAggregate
            DBSPMapIndexOperator index = i.node().as(DBSPMapIndexOperator.class);
            if (index == null) {
                super.postorder(operator);
                return;
            }
            DBSPClosureExpression indexClosure = index.getClosureFunction();

            List<DBSPType> aggregatedTypes = new ArrayList<>();
            List<DBSPType> accumulatorTypes = new ArrayList<>();
            for (MinMaxAggregate mmAggregate: aggregates) {
                DBSPType resultType = mmAggregate.type;
                accumulatorTypes.add(resultType);

                DBSPExpression expr = mmAggregate.increment.body;
                if (expr.is(DBSPCastExpression.class))
                    expr = expr.to(DBSPCastExpression.class).source;
                DBSPConditionalAggregateExpression ca = expr.to(DBSPConditionalAggregateExpression.class);
                DBSPExpression aggregatedField = ca.right;
                DBSPType aggregationInputType = aggregatedField.getType();
                aggregatedTypes.add(aggregationInputType);
            }

            List<DBSPExpression> indexExpressions = new ArrayList<>();
            List<DBSPExpression> inits = new ArrayList<>();
            List<DBSPExpression> comparisons = new ArrayList<>();

            DBSPVariablePath acc = new DBSPTypeTuple(accumulatorTypes).var();
            DBSPVariablePath inputVar = new DBSPTypeTuple(aggregatedTypes).ref().var();
            int ix = 0;
            for (MinMaxAggregate mmAggregate: aggregates) {
                DBSPOpcode code = mmAggregate.isMin ? DBSPOpcode.AGG_MIN : DBSPOpcode.AGG_MAX;
                // The mmAggregate.increment function has the following shape:
                // conditional_aggregate(accumulator, aggregatedValue, null).closure(accumulator, inputRow, weight)
                DBSPClosureExpression increment = mmAggregate.increment;
                DBSPParameter[] parameters = increment.parameters;
                Utilities.enforce(parameters.length == 3);
                DBSPType resultType = mmAggregate.type;

                // Need to index by (Key, Value), where Value is the value that is being aggregated.
                // If the function that extracts the aggregation field from the indexed row
                // |value| -> aggregatedField...
                // ... the index closure has the shape |row| -> (key(row), value(row))
                indexExpressions.add(mmAggregate.aggregatedValue.call(
                        indexClosure.body.field(1).borrow()).reduce(this.compiler()));

                DBSPExpression init = inputVar.deref().field(ix).applyCloneIfNeeded()
                        .cast(CalciteObject.EMPTY, resultType, false);
                inits.add(init);

                accumulatorTypes.add(resultType);

                DBSPExpression comparison =
                        new DBSPBinaryExpression(operator.getNode(),
                                resultType, code, acc.field(ix).applyCloneIfNeeded(),
                                inputVar.deref().field(ix).applyCloneIfNeeded())
                                .cast(CalciteObject.EMPTY, resultType, false);
                comparisons.add(comparison);
                ix++;
            }
            // Need to build the closure |row| -> (key(row), aggregatedField(value(row)))
            DBSPClosureExpression newIndexClosure =
                    new DBSPRawTupleExpression(
                            indexClosure.body.field(0),
                            new DBSPTupleExpression(indexExpressions, false))
                            .closure(indexClosure.parameters);

            OutputPort indexInput = index.input();
            DBSPMapIndexOperator reIndex = new DBSPMapIndexOperator(
                    index.getRelNode(), newIndexClosure, indexInput.simpleNode().outputPort());
            this.addOperator(reIndex);

            DBSPClosureExpression init = new DBSPTupleExpression(inits, false)
                    .closure(inputVar, this.weightVar);
            DBSPClosureExpression comparison = new DBSPTupleExpression(comparisons, false)
                    .closure(acc, inputVar, this.weightVar);

            DBSPSimpleOperator chain = new DBSPChainAggregateOperator(operator.getRelNode(),
                    init, comparison, operator.outputType, reIndex.outputPort());
            this.map(operator, chain);
        }
    }
}
