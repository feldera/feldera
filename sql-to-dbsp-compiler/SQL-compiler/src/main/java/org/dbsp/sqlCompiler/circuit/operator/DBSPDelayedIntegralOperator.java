package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;

import java.util.List;

/** This operator is like an integral followed by a delay.
 * This shows up often, and it can be implemented more efficiently
 * than using the pair. */
@NonCoreIR
public final class DBSPDelayedIntegralOperator extends DBSPUnaryOperator {
    public DBSPDelayedIntegralOperator(CalciteObject node, OutputPort source) {
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
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPDelayedIntegralOperator(
                    this.getNode(), newInputs.get(0));
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPDelayedIntegralOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPDelayedIntegralOperator(CalciteObject.EMPTY, info.getInput(0));
    }
}
