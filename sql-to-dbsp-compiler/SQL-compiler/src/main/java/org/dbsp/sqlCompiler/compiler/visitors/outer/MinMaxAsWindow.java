package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.aggregate.AggregateBase;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;

import java.util.function.Predicate;

/** Detects a pattern in the circuit and replaces it with a Window operator.
 * The pattern detected is:
 * index -> min/max -> deindex -> aggregate_zero.
 * The source of the index must be an append-only stream. */
public class MinMaxAsWindow extends Passes {
    final AppendOnly appendOnly;

    public MinMaxAsWindow(IErrorReporter reporter) {
        super(reporter);
        this.appendOnly = new AppendOnly(reporter);
        this.add(this.appendOnly);
        this.add(new ExpandMaxAsWindow(reporter, this.appendOnly::isAppendOnly));
    }

    static class ExpandMaxAsWindow extends CircuitCloneVisitor {
        final Predicate<DBSPOperator> isAppendOnly;

        public ExpandMaxAsWindow(IErrorReporter reporter, Predicate<DBSPOperator> isAppendOnly) {
            super(reporter, false);
            this.isAppendOnly = isAppendOnly;
        }

        @Override
        public void postorder(DBSPAggregateZeroOperator operator) {
            DBSPOperator i = this.mapped(operator.input());
            DBSPDeindexOperator deindex;
            DBSPStreamAggregateOperator max = null;
            DBSPMapIndexOperator index = null;
            deindex = i.as(DBSPDeindexOperator.class);
            if (deindex != null) {
                i = deindex.input();
                max = i.as(DBSPStreamAggregateOperator.class);
            }
            if (max != null) {
                i = max.input();
                index = i.as(DBSPMapIndexOperator.class);
            }
            if (index == null) {
                super.postorder(operator);
                return;
            }
            DBSPOperator source = index.input();
            if (!this.isAppendOnly.test(source)) {
                super.postorder(operator);
                return;
            }

            MinMaxAggregate maxAggregate = null;
            DBSPAggregate aggregate = max.getAggregate();
            if (aggregate.size() == 1) {
                AggregateBase agg = aggregate.aggregates.get(0);
                maxAggregate = agg.as(MinMaxAggregate.class);
            }
            if (maxAggregate == null) {
                super.postorder(operator);
                return;
            }

            DBSPOpcode code = maxAggregate.isMin ? DBSPOpcode.MIN : DBSPOpcode.MAX;
            DBSPParameter[] parameters = maxAggregate.increment.parameters;
            assert parameters.length == 3;
            DBSPParameter inputRow = parameters[1];
            DBSPType valueType = inputRow.type;
            DBSPType resultType = maxAggregate.type;
            DBSPClosureExpression extractTs = maxAggregate.aggregatedValue.closure(
                    inputRow, new DBSPTypeRawTuple().ref().var().asParameter());
            DBSPVariablePath left = resultType.ref().var();
            DBSPVariablePath right = resultType.ref().var();
            DBSPExpression comparison = ExpressionCompiler.makeBinaryExpression(operator.getNode(),
                    valueType, code, left.deref(), right.deref());
            assert resultType.mayBeNull;
            DBSPClosureExpression init = resultType.none().closure();
            // TODO: this is wrong, since we need a ZSet as output, and the waterline does not produce it
            DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                    operator.getNode(), init, extractTs,
                    comparison.closure(left.asParameter(), right.asParameter()), source);
            this.map(operator, waterline);
        }
    }
}
