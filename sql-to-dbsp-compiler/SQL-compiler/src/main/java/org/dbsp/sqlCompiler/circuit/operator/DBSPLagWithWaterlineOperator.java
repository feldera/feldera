package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** A LAG or LEAD operator, like {@link DBSPLagOperator}, that garbage collects its state below
 * the waterline of its second input.  The rows at or above the waterline must come last in each
 * partition of the order of {@code comparator}. */
public final class DBSPLagWithWaterlineOperator extends DBSPBinaryOperator
        implements IHasInputIntegrator, IHasPostIntegrator, IIncremental {
    // Usually a DBSPComparatorExpression, replaced with a PathExpression later.
    public final DBSPExpression comparator;
    public final DBSPExpression projection;
    /** Closure that takes a row and the waterline, and returns true if the row is at or above
     * the waterline. */
    public final DBSPClosureExpression aboveWaterline;
    public final int offset;

    /**
     * Create a LEAD/LAG window aggregation operator that garbage collects its state.
     *
     * @param node           Calcite object that is being compiled.
     * @param offset         Lead/lag offset.
     * @param projection     Projection that computes the delayed row from Option[input row].
     * @param function       Expression that produces the output from two arguments:
     *                       the current row and the delayed row.
     * @param comparator     Comparator used for sorting.
     * @param aboveWaterline Closure that takes a row and the waterline, and returns true if the
     *                       row is at or above the waterline.
     * @param outputType     Type of output record produced.
     * @param data           Input node for the lag operator.
     * @param waterline      Waterline of the first column that the comparator compares, in a
     *                       TypedBox.
     */
    public DBSPLagWithWaterlineOperator(
            CalciteRelNode node, int offset, DBSPExpression projection, DBSPExpression function,
            DBSPExpression comparator, DBSPClosureExpression aboveWaterline,
            DBSPTypeIndexedZSet outputType, OutputPort data, OutputPort waterline) {
        super(node, "lag_custom_order_with_waterline", function, outputType, data, waterline);
        Utilities.enforce(comparator.is(DBSPComparatorExpression.class) ||
                comparator.is(DBSPPathExpression.class));
        if (comparator.is(DBSPComparatorExpression.class)) {
            Utilities.enforce(comparator.to(DBSPComparatorExpression.class)
                    .comparedValueType()
                    .sameType(data.getOutputIndexedZSetType().elementType),
                    () -> "Comparator type " + comparator + "\ndoes not match element type for LAG\n" +
                    data.getOutputIndexedZSetType().elementType);
        }
        Utilities.enforce(aboveWaterline.parameters.length == 2,
                () -> "Expected 2 parameters for " + aboveWaterline);
        this.comparator = comparator;
        this.projection = projection;
        this.aboveWaterline = aboveWaterline;
        this.offset = offset;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPLagWithWaterlineOperator otherOperator = other.as(DBSPLagWithWaterlineOperator.class);
        if (otherOperator == null)
            return false;
        return this.comparator.equivalent(otherOperator.comparator) &&
                this.projection.equivalent(otherOperator.projection) &&
                this.aboveWaterline.equivalent(otherOperator.aboveWaterline) &&
                this.offset == otherOperator.offset;
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            Utilities.enforce(newInputs.size() == 2, () -> "Expected 2 inputs " + newInputs);
            return new DBSPLagWithWaterlineOperator(this.getRelNode(), this.offset,
                    this.projection, toClosure(function), this.comparator, this.aboveWaterline,
                    outputType.to(DBSPTypeIndexedZSet.class), newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        }
        return this;
    }

    @Override
    public boolean hasInputIntegrator(int inputIndex) {
        // Input 1 carries the waterline
        return inputIndex == 0;
    }

    @Override
    public boolean garbageCollectsInput(int inputIndex) {
        return inputIndex == 0;
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
        visitor.property("projection");
        this.projection.accept(visitor);
        visitor.property("aboveWaterline");
        this.aboveWaterline.accept(visitor);
    }

    @Override
    protected StreamKind computeOutputKind() {
        this.requireInputAmong(0, StreamKind.DELTA);
        this.requireInputAmong(1, StreamKind.WATERLINE);
        return StreamKind.DELTA;
    }

    @SuppressWarnings("unused")
    public static DBSPLagWithWaterlineOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        int offset = Utilities.getIntProperty(node, "offset");
        DBSPExpression comparator = fromJsonInner(node, "comparator", decoder, DBSPExpression.class);
        DBSPExpression projection = fromJsonInner(node, "projection", decoder, DBSPExpression.class);
        DBSPClosureExpression aboveWaterline = fromJsonInner(
                node, "aboveWaterline", decoder, DBSPClosureExpression.class);
        return new DBSPLagWithWaterlineOperator(
                CalciteEmptyRel.INSTANCE, offset, projection, info.getFunction(),
                comparator, aboveWaterline, info.getIndexedZsetType(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPLagWithWaterlineOperator.class);
    }
}
