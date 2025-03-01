package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import java.util.List;

/** Implements the LAG/LEAD operators for an SQL OVER Window */
public final class DBSPLagOperator extends DBSPUnaryOperator {
    public final DBSPComparatorExpression comparator;
    public final DBSPExpression projection;
    public final int offset;

    /**
     * Create a LEAD/LAG window aggregation operator.
     *
     * @param node       Calcite object that is being compiled.
     * @param offset     Lead/lag offset.
     * @param projection Projection that computes the delayed row from Option<input row>.
     * @param function   Expression that produces the output from two arguments:
     *                   the current row and the delayed row.
     * @param comparator Comparator used for sorting.
     * @param outputType Type of output record produced.
     * @param source     Input node for the lag operator.
     */
    public DBSPLagOperator(CalciteRelNode node, int offset,
                           DBSPExpression projection, DBSPExpression function,
                           DBSPComparatorExpression comparator,
                           DBSPTypeIndexedZSet outputType, OutputPort source) {
        super(node, "lag_custom_order", function, outputType, source.isMultiset(), source);
        this.comparator = comparator;
        this.projection = projection;
        this.offset = offset;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPLagOperator otherOperator = other.as(DBSPLagOperator.class);
        if (otherOperator == null)
            return false;
        return this.comparator.equivalent(otherOperator.comparator) &&
                this.projection.equivalent(otherOperator.projection) &&
                this.offset == otherOperator.offset;
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        assert newInputs.size() == 1: "Expected 1 input " + newInputs;
        if (force || this.inputsDiffer(newInputs)) {
            return new DBSPLagOperator(this.getRelNode(), this.offset,
                    this.projection, this.getFunction(), this.comparator,
                    this.getOutputIndexedZSetType(), newInputs.get(0))
                    .copyAnnotations(this);
        }
        return this;
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
    }

    @SuppressWarnings("unused")
    public static DBSPLagOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        int offset = Utilities.getIntProperty(node, "offset");
        DBSPComparatorExpression comparator = fromJsonInner(node, "comparator", decoder, DBSPComparatorExpression.class);
        DBSPExpression projection = fromJsonInner(node, "projection", decoder, DBSPExpression.class);
        return new DBSPLagOperator(
                CalciteEmptyRel.INSTANCE, offset, projection, info.getFunction(),
                comparator, info.getIndexedZsetType(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPLagOperator.class);
    }
}
