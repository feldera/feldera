package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;

/** This operator is like an integral followed by a delay.  It exists only in the expansion
 * circuit that the monotonicity analysis (DeltaExpandOperators) builds. */
@NonCoreIR
public final class DBSPDelayedIntegralOperator extends DBSPUnaryOperator implements IContainsIntegrator {
    public DBSPDelayedIntegralOperator(CalciteRelNode node, OutputPort source) {
        super(node, "accumulate_delay_trace", null, source.outputType(), source.isMultiset(), source);
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
        return new DBSPDelayedIntegralOperator(CalciteEmptyRel.INSTANCE, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPDelayedIntegralOperator.class);
    }
}
