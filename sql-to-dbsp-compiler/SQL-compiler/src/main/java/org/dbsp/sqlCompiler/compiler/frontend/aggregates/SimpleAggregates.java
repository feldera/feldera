package org.dbsp.sqlCompiler.compiler.frontend.aggregates;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.sql.SqlKind;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.Objects;

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

        // Index the previous input using the group keys
        DBSPTypeIndexedZSet localGroupAndInput = TypeCompiler.makeIndexedZSet(groupKeyType, inputType);
        DBSPVariablePath rowVar = inputType.ref().var();
        DBSPExpression[] expressions = new DBSPExpression[]{rowVar.deref()};
        DBSPTupleExpression flattened = DBSPTupleExpression.flatten(expressions);
        DBSPClosureExpression makeKeys =
                new DBSPRawTupleExpression(
                        new DBSPTupleExpression(
                                Linq.map(this.partitionKeys,
                                        p -> rowVar.deref().field(p).applyCloneIfNeeded()), false),
                        new DBSPTupleExpression(this.node,
                                lastOperator.getOutputZSetElementType().to(DBSPTypeTuple.class),
                                Objects.requireNonNull(flattened.fields)))
                        .closure(rowVar);
        DBSPSimpleOperator indexedInput = new DBSPMapIndexOperator(
                node, makeKeys, localGroupAndInput, lastOperator.outputPort());
        this.compiler.addOperator(indexedInput);

        DBSPSimpleOperator aggregate = this.compiler.implementAggregateList(
                this.node, groupKeyType, indexedInput.outputPort(), aggregates);

        // Join again with the indexed input
        DBSPVariablePath key = groupKeyType.ref().var();
        DBSPVariablePath left = flattened.getType().ref().var();
        DBSPVariablePath right = aggregate.getOutputIndexedZSetType().elementType.ref().var();
        DBSPClosureExpression append =
                DBSPTupleExpression.flatten(left.deref(), right.deref()).closure(
                        key, left, right);
        CalciteRelNode n = this.node;
        if (isLast)
            n = this.node.getFinal();
        // Do not insert the last operator
        return new DBSPStreamJoinOperator(n, TypeCompiler.makeZSet(append.getResultType()),
                append, true, indexedInput.outputPort(), aggregate.outputPort());
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
