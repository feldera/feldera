package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.aggregate.AggregateBase;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalAggregateExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import java.util.function.Predicate;

/** Optimize the implementation of Min and Max aggregates.
 * Currently only optimized min and max for append-only streams. */
public class MinMaxOptimize extends Passes {
    final AppendOnly appendOnly;

    public MinMaxOptimize(IErrorReporter reporter, DBSPVariablePath weightVar) {
        super(reporter);
        this.appendOnly = new AppendOnly(reporter);
        this.add(this.appendOnly);
        this.add(new ExpandMaxAsWindow(reporter, weightVar, this.appendOnly::isAppendOnly));
    }

    static class ExpandMaxAsWindow extends CircuitCloneVisitor {
        final Predicate<DBSPOperator> isAppendOnly;
        final DBSPVariablePath weightVar;

        public ExpandMaxAsWindow(IErrorReporter reporter, DBSPVariablePath weightVar,
                                 Predicate<DBSPOperator> isAppendOnly) {
            super(reporter, false);
            this.isAppendOnly = isAppendOnly;
            this.weightVar = weightVar;
        }

        @Override
        public void postorder(DBSPStreamAggregateOperator operator) {
            DBSPOperator i = this.mapped(operator.input());
            if (!this.isAppendOnly.test(operator.input())) {
                super.postorder(operator);
                return;
            }
            DBSPMapIndexOperator index = i.as(DBSPMapIndexOperator.class);
            if (index == null) {
                super.postorder(operator);
                return;
            }

            MinMaxAggregate mmAggregate = null;
            DBSPAggregate aggregate = operator.getAggregate();
            if (aggregate.size() == 1) {
                AggregateBase agg = aggregate.aggregates.get(0);
                mmAggregate = agg.as(MinMaxAggregate.class);
            }
            if (mmAggregate == null) {
                super.postorder(operator);
                return;
            }

            DBSPOpcode code = mmAggregate.isMin ? DBSPOpcode.AGG_MIN : DBSPOpcode.AGG_MAX;
            // The mmAggregate.increment function has the following shape:
            // conditional_aggregate(accumulator, aggregatedValue, null).closure(accumulator, inputRow, weight)
            DBSPClosureExpression increment = mmAggregate.increment;
            DBSPParameter[] parameters = increment.parameters;
            assert parameters.length == 3;
            DBSPType resultType = mmAggregate.type;

            // Need to index by (Key, Value), where Value is the value that is being aggregated.
            DBSPConditionalAggregateExpression ca = increment.body.to(DBSPConditionalAggregateExpression.class);
            DBSPExpression aggregatedField = ca.right;
            DBSPType aggregationInputType = aggregatedField.getType();
            // This if the function that extracts the aggregation field from the indexed row
            // |value| -> aggregatedField
            DBSPClosureExpression extractAggField = aggregatedField.closure(increment.parameters[1]);
            // The index closure has the shape |row| -> (key(row), value(row))
            DBSPClosureExpression indexClosure = index.getClosureFunction();
            // Need to build the closure |row| -> (key(row), aggregatedField(value(row)))
            DBSPClosureExpression newIndexClosure =
                    new DBSPRawTupleExpression(
                            indexClosure.body.field(0),
                            new DBSPTupleExpression(
                                    extractAggField.call(indexClosure.body.field(1).borrow())))
                            .closure(indexClosure.parameters)
                            .reduce(this.errorReporter)
                            .to(DBSPClosureExpression.class);

            DBSPOperator indexInput = index.input();
            DBSPTypeIndexedZSet outputType = new DBSPTypeIndexedZSet(
                    index.getNode(), index.getKeyType(), new DBSPTypeTuple(aggregatedField.getType()));
            DBSPMapIndexOperator reIndex = new DBSPMapIndexOperator(
                    index.getNode(), newIndexClosure, outputType, index.isMultiset, indexInput);
            this.addOperator(reIndex);

            DBSPVariablePath inputVar = new DBSPTypeTuple(aggregationInputType).ref().var();
            DBSPClosureExpression init = new DBSPTupleExpression(
                    inputVar.deref().field(0).cast(resultType))
                    .closure(inputVar, this.weightVar);

            DBSPVariablePath acc = new DBSPTypeTuple(resultType).var();
            DBSPClosureExpression comparison =
                    new DBSPTupleExpression(new DBSPBinaryExpression(operator.getNode(),
                            resultType, code, acc.field(0), inputVar.deref().field(0))
                            .cast(resultType))
                            .closure(acc, inputVar, this.weightVar);

            DBSPOperator chain = new DBSPChainAggregateOperator(operator.getNode(),
                    init, comparison, operator.outputType, reIndex);
            this.map(operator, chain);
        }
    }
}
