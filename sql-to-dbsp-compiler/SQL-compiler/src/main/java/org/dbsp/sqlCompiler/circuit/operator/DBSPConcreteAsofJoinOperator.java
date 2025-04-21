package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** This operator is purely incremental, it does not have a non-incremental form.
 * It corresponds to the DBSP asof_join operator. */
public final class DBSPConcreteAsofJoinOperator extends DBSPJoinBaseOperator {
    // Usually a DBSPComparatorExpression; currently not used
    // https://github.com/feldera/feldera/issues/2212
    public final DBSPExpression comparator;
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
     * @param comparator      Function that compares two timestamps.  Not yet used - hardwired.
     * @param isMultiset      True if output is a multiset
     * @param isLeft          True if this is a left join
     * @param left            Left input
     * @param right           Right input
     */
    public DBSPConcreteAsofJoinOperator(CalciteRelNode node, DBSPTypeZSet outputType,
                                        DBSPExpression function,
                                        DBSPClosureExpression leftTimestamp,
                                        DBSPClosureExpression rightTimestamp,
                                        DBSPExpression comparator,
                                        boolean isMultiset, boolean isLeft,
                                        OutputPort left, OutputPort right) {
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
        DBSPType[] argumentTypes = function.getType().to(DBSPTypeFunction.class).parameterTypes;
        assert argumentTypes.length == 3;
        assert argumentTypes[1].sameType(leftTimestamp.parameters[0].getType());
        assert argumentTypes[2].sameType(rightTimestamp.parameters[0].getType().withMayBeNull(true));
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        super.accept(visitor);
        visitor.property("leftTimestamp");
        this.leftTimestamp.accept(visitor);
        visitor.property("rightTimestamp");
        this.rightTimestamp.accept(visitor);
        visitor.property("comparator");
        this.comparator.accept(visitor);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPConcreteAsofJoinOperator(this.getRelNode(),
                outputType.to(DBSPTypeZSet.class), Objects.requireNonNull(expression),
                this.leftTimestamp, this.rightTimestamp,
                this.comparator, this.isMultiset, this.isLeft, this.left(), this.right());
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        assert newInputs.size() == 2;
        if (force || this.inputsDiffer(newInputs))
            return new DBSPConcreteAsofJoinOperator(
                    this.getRelNode(), this.getOutputZSetType(),
                    this.getFunction(), this.leftTimestamp, this.rightTimestamp,
                    this.comparator, this.isMultiset, this.isLeft,
                    newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        return this;
    }

    @Override
    public DBSPJoinBaseOperator withFunctionAndInputs(DBSPExpression function, OutputPort left, OutputPort right) {
        return new DBSPConcreteAsofJoinOperator(this.getRelNode(), this.getOutputZSetType(), function,
                this.leftTimestamp, this.rightTimestamp, this.comparator, this.isMultiset, this.isLeft, left, right);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPConcreteAsofJoinOperator otherOperator = other.as(DBSPConcreteAsofJoinOperator.class);
        if (otherOperator == null)
            return false;
        return this.leftTimestamp.equivalent(otherOperator.leftTimestamp) &&
                this.rightTimestamp.equivalent(otherOperator.rightTimestamp) &&
                this.comparator.equivalent(otherOperator.comparator);
    }

    @SuppressWarnings("unused")
    public static DBSPConcreteAsofJoinOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPClosureExpression leftTimestamp = fromJsonInner(node, "leftTimestamp", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression rightTimestamp = fromJsonInner(node, "rightTimestamp", decoder, DBSPClosureExpression.class);
        DBSPExpression comparator = fromJsonInner(node, "comparator", decoder, DBSPExpression.class);
        boolean isLeft = Utilities.getBooleanProperty(node, "isLeft");
        return new DBSPConcreteAsofJoinOperator(
                CalciteEmptyRel.INSTANCE, info.getZsetType(), info.getFunction(),
                leftTimestamp, rightTimestamp, comparator,
                info.isMultiset(), isLeft, info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPConcreteAsofJoinOperator.class);
    }
}
