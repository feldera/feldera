package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** This operator is purely incremental, it does not have a non-incremental form */
public final class DBSPAsofJoinOperator extends DBSPBinaryOperator {
    public final DBSPComparatorExpression comparator;
    public final DBSPClosureExpression leftTimestamp;
    public final DBSPClosureExpression rightTimestamp;
    public final boolean isLeft;

    /**
     * Create an ASOF join operator
     *
     * @param node            Calcite node
     * @param outputType      Output type of operator
     * @param function        Closure from key, valueLeft, valueRight to result type
     * @param leftTimestamp   Function that extracts a "timestamp" from the left input
     * @param rightTimestamp  Function that extracts a "timestamp" from the right input
     * @param comparator      Function that compares two timestamps
     * @param isMultiset      True if output is a multiset
     * @param isLeft          True if this is a left join
     * @param left            Left input
     * @param right           Right input
     */
    public DBSPAsofJoinOperator(CalciteObject node, DBSPTypeZSet outputType,
                                DBSPExpression function,
                                DBSPClosureExpression leftTimestamp,
                                DBSPClosureExpression rightTimestamp,
                                DBSPComparatorExpression comparator,
                                boolean isMultiset, boolean isLeft,
                                DBSPOperator left, DBSPOperator right) {
        super(node, "asof_join", function, outputType, isMultiset, left, right);
        this.isLeft = isLeft;
        this.comparator = comparator;
        this.leftTimestamp = leftTimestamp;
        this.rightTimestamp = rightTimestamp;
        //   pub fn asof_join<TS, F, TSF1, TSF2, V2, V>(
        //        &self,
        //        other: &Stream<RootCircuit, OrdIndexedZSet<K1, V2>>,
        //        join: F,
        //        ts_func1: TSF1,
        //        ts_func2: TSF2,
        //    ) -> Stream<RootCircuit, OrdZSet<V>>
        //    where
        //        TS: DBData,
        //        V2: DBData,
        //        V: DBData,
        //        F: Fn(&K1, &V1, Option<&V2>) -> V + Clone + 'static,
        //        TSF1: Fn(&V1) -> TS + Clone + 'static,
        //        TSF2: Fn(&V2) -> TS + 'static,
        DBSPType elementResultType = this.getOutputZSetElementType();
        this.checkResultType(function, elementResultType);
        assert leftTimestamp.getResultType().sameType(rightTimestamp.getResultType());
        DBSPType[] argumentTypes = function.getType().to(DBSPTypeFunction.class).argumentTypes;
        assert argumentTypes.length == 3;
        assert argumentTypes[1].sameType(leftTimestamp.parameters[0].getType());
        assert argumentTypes[2].sameType(rightTimestamp.parameters[0].getType().setMayBeNull(true));
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    public DBSPType getKeyType() {
        return this.left().getOutputIndexedZSetType().keyType;
    }

    public DBSPType getLeftInputValueType() {
        return this.left().getOutputIndexedZSetType().elementType;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPAsofJoinOperator(this.getNode(),
                outputType.to(DBSPTypeZSet.class), Objects.requireNonNull(expression),
                this.leftTimestamp, this.rightTimestamp,
                this.comparator, this.isMultiset, this.isLeft, this.left(), this.right());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2;
        if (force || this.inputsDiffer(newInputs))
            return new DBSPAsofJoinOperator(
                    this.getNode(), this.getOutputZSetType(),
                    this.getFunction(), this.leftTimestamp, this.rightTimestamp,
                    this.comparator, this.isMultiset, this.isLeft,
                    newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        return this;
    }

    public int getLeftTimestampIndex() {
        // This relies on the leftTimestamp function having a very specific shape
        DBSPExpression body = this.leftTimestamp.body;
        if (body.is(DBSPCastExpression.class))
            body = body.to(DBSPCastExpression.class).source;
        return body.to(DBSPFieldExpression.class).fieldNo;
    }

    public int getRightTimestampIndex() {
        // This relies on the rightTimestamp function having a very specific shape
        DBSPExpression body = this.rightTimestamp.body;
        if (body.is(DBSPCastExpression.class))
            body = body.to(DBSPCastExpression.class).source;
        return body.to(DBSPFieldExpression.class).fieldNo;
    }
}
