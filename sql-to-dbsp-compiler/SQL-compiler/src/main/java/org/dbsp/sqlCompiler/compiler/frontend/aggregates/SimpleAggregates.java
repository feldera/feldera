package org.dbsp.sqlCompiler.compiler.frontend.aggregates;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.sql.SqlKind;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Utilities;

/**
 * Simple aggregates used in an OVER, no LAG, or RANGE.
 */
public class SimpleAggregates extends WindowAggregates {
    protected SimpleAggregates(CalciteToDBSPCompiler compiler, Window window, Window.Group group, int windowFieldIndex) {
        super(compiler, window, group, windowFieldIndex);
        Utilities.enforce(this.group.orderKeys.getFieldCollations().isEmpty());
        Utilities.enforce(isUnbounded(group));
    }

    @Override
    public DBSPSimpleOperator implement(DBSPSimpleOperator unusedInput, DBSPSimpleOperator lastOperator, boolean isLast) {
        DBSPTypeTuple tuple = this.windowResultType.slice(
                this.windowFieldIndex, this.windowFieldIndex + this.aggregateCalls.size());
        DBSPType groupKeyType = this.partitionKeys().getType();
        DBSPType inputType = lastOperator.getOutputZSetElementType();

        DBSPAggregateList aggregates = this.compiler.createAggregates(
                this.compiler.compiler(),
                this.window, this.aggregateCalls, this.window.constants, tuple, inputType,
                0, this.group.keys, true);

        OutputPort indexedInput = this.indexInput(lastOperator);

        DBSPSimpleOperator aggregate = this.compiler.implementAggregateList(
                this.node, groupKeyType, indexedInput, aggregates);

        // Join again with the indexed input
        DBSPVariablePath key = groupKeyType.ref().var();
        DBSPVariablePath left = indexedInput.getOutputIndexedZSetType().elementType.ref().var();
        DBSPVariablePath right = aggregate.getOutputIndexedZSetType().elementType.ref().var();
        DBSPClosureExpression append =
                DBSPTupleExpression.flatten(left.deref(), right.deref()).closure(
                        key, left, right);
        // Do not insert the last operator
        return new DBSPStreamJoinOperator(this.node.maybeFinal(isLast), TypeCompiler.makeZSet(append.getResultType()),
                append, true, indexedInput, aggregate.outputPort());
    }

    @Override
    public boolean isCompatible(AggregateCall call) {
        SqlKind kind = call.getAggregation().getKind();
        return kind != SqlKind.LAG &&
                kind != SqlKind.LEAD &&
                kind != SqlKind.FIRST_VALUE &&
                kind != SqlKind.LAST_VALUE;
    }
}
