package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** A high-level representation of the ASOF JOIN.  Converted to a pair of
 * {@link DBSPMapIndexOperator} and one {@link DBSPConcreteAsofJoinOperator}. */
public final class DBSPAsofJoinOperator extends DBSPJoinBaseOperator {
    // Usually a DBSPComparatorExpression; currently not used
    // https://github.com/feldera/feldera/issues/2212
    public final DBSPExpression comparator;
    public final int leftTimestampIndex;
    public final int rightTimestampIndex;
    public final boolean isLeft;

    /**
     * Create an ASOF join operator
     *
     * @param node            Calcite node
     * @param outputType      Output type of operator
     * @param function        Closure from key, valueLeft, valueRight.nullableCast()  to result type
     * @param leftTimestampIndex Index of left timestamp in left tuple.
     * @param rightTimestampIndex Index of right timestamp in right tuple
     * @param comparator      Function that compares two timestamps.
     * @param isMultiset      True if output is a multiset
     * @param isLeft          True if this is a left join
     * @param left            Left input
     * @param right           Right input
     */
    public DBSPAsofJoinOperator(CalciteRelNode node, DBSPTypeZSet outputType,
                                DBSPExpression function,
                                int leftTimestampIndex,
                                int rightTimestampIndex,
                                DBSPExpression comparator,
                                boolean isMultiset, boolean isLeft,
                                OutputPort left, OutputPort right) {
        super(node, "asof_join_abstract", function, outputType, isMultiset, left, right);
        this.isLeft = isLeft;
        this.comparator = comparator;
        this.leftTimestampIndex = leftTimestampIndex;
        this.rightTimestampIndex = rightTimestampIndex;
        DBSPType elementResultType = this.getOutputZSetElementType();
        this.checkResultType(function, elementResultType);

        // Note that the third argument of the function may not match in type the right join input:
        // if the join is left join, the argument is always nullable, even if the input is not.
        DBSPType[] argumentTypes = function.getType().to(DBSPTypeFunction.class).parameterTypes;
        assert argumentTypes.length == 3;
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
        visitor.property("comparator");
        this.comparator.accept(visitor);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPAsofJoinOperator(this.getRelNode(),
                outputType.to(DBSPTypeZSet.class), Objects.requireNonNull(expression),
                this.leftTimestampIndex, this.rightTimestampIndex,
                this.comparator, this.isMultiset, this.isLeft, this.left(), this.right());
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        assert newInputs.size() == 2;
        if (force || this.inputsDiffer(newInputs))
            return new DBSPAsofJoinOperator(
                    this.getRelNode(), this.getOutputZSetType(),
                    this.getFunction(), this.leftTimestampIndex, this.rightTimestampIndex,
                    this.comparator, this.isMultiset, this.isLeft,
                    newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        return this;
    }

    @Override
    public DBSPJoinBaseOperator withFunctionAndInputs(DBSPExpression function, OutputPort left, OutputPort right) {
        return new DBSPAsofJoinOperator(this.getRelNode(), this.getOutputZSetType(), function,
                this.leftTimestampIndex, this.rightTimestampIndex,
                this.comparator, this.isMultiset, this.isLeft, left, right);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPAsofJoinOperator otherOperator = other.as(DBSPAsofJoinOperator.class);
        if (otherOperator == null)
            return false;
        return this.leftTimestampIndex == otherOperator.leftTimestampIndex &&
                this.rightTimestampIndex == otherOperator.rightTimestampIndex &&
                this.comparator.equivalent(otherOperator.comparator);
    }

    @SuppressWarnings("unused")
    public static DBSPAsofJoinOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        int leftTimestampIndex = Utilities.getIntProperty(node, "leftTimestampIndex");
        int rightTimestampIndex = Utilities.getIntProperty(node, "rightTimestampIndex");
        DBSPExpression comparator = fromJsonInner(node, "comparator", decoder, DBSPExpression.class);
        boolean isLeft = Utilities.getBooleanProperty(node, "isLeft");
        return new DBSPAsofJoinOperator(
                CalciteEmptyRel.INSTANCE, info.getZsetType(), info.getFunction(),
                leftTimestampIndex, rightTimestampIndex, comparator,
                info.isMultiset(), isLeft, info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPAsofJoinOperator.class);
    }
}
