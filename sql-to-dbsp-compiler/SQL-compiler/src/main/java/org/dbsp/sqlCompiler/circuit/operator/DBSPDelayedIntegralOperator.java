package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

/** This operator is like an integral followed by a delay.
 * This shows up often, and it can be implemented more efficiently
 * than using the pair. */
@NonCoreIR
public final class DBSPDelayedIntegralOperator extends DBSPUnaryOperator {
    public DBSPDelayedIntegralOperator(CalciteRelNode node, OutputPort source) {
        super(node, "delay_trace", null, source.outputType(), source.isMultiset(), source);
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
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPDelayedIntegralOperator(
                    this.getRelNode(), newInputs.get(0));
        }
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPDelayedIntegralOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPDelayedIntegralOperator(CalciteEmptyRel.INSTANCE, info.getInput(0));
    }
}
