package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Implements the table function HOP.  This is desugared into a pair of
 * operators: map (computing the hop start window) followed by flat_map
 * (which generates all the windows).  It does not correspond to any DBSP
 * Rust operator. */
public final class DBSPHopOperator extends DBSPUnaryOperator {
    public final int timestampIndex;
    public final DBSPExpression interval;
    public final DBSPExpression start;
    public final DBSPExpression size;

    public DBSPHopOperator(CalciteRelNode node, int timestampIndex,
                           DBSPExpression interval,
                           DBSPExpression start, DBSPExpression size,
                           DBSPTypeZSet outputType, OutputPort input) {
        super(node, "hop", null, outputType, input.isMultiset(), input);
        this.timestampIndex = timestampIndex;
        this.interval = interval;
        this.start = start;
        this.size = size;
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
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPHopOperator hop = other.as(DBSPHopOperator.class);
        if (hop == null)
            return false;
        return EquivalenceContext.equiv(this.interval, hop.interval) &&
                this.timestampIndex == hop.timestampIndex &&
                EquivalenceContext.equiv(this.start, hop.start) &&
                EquivalenceContext.equiv(this.size, hop.size);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPHopOperator(
                this.getRelNode(), this.timestampIndex, this.interval, this.start, this.size,
                outputType.to(DBSPTypeZSet.class), this.input())
                .copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPHopOperator(
                    this.getRelNode(), this.timestampIndex, this.interval, this.start, this.size,
                    this.getOutputZSetType(), newInputs.get(0))
                    .copyAnnotations(this);
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPHopOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        int timestampIndex = Utilities.getIntProperty(node, "timestampIndex");
        DBSPExpression interval = fromJsonInner(node, "interval", decoder, DBSPExpression.class);
        DBSPExpression start = fromJsonInner(node, "start", decoder, DBSPExpression.class);
        DBSPExpression size = fromJsonInner(node, "size", decoder, DBSPExpression.class);
        return new DBSPHopOperator(CalciteEmptyRel.INSTANCE, timestampIndex, interval, start, size,
                info.getZsetType(), info.getInput(0));
    }
}
