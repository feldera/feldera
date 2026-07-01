package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Operator that panics at runtime if the running integral of its input stream
 * contains any record with a non-positive weight. */
public final class DBSPWeightValidatorOperator extends DBSPUnaryOperator {
    /** Panic message. */
    public final String message;

    public DBSPWeightValidatorOperator(CalciteRelNode node, OutputPort source, String message) {
        super(node, "integral_weight_validator", null, source.outputType(), source.isMultiset(), source);
        this.message = message;
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
            return new DBSPWeightValidatorOperator(
                    this.getRelNode(), newInputs.get(0), this.message).copyAnnotations(this);
        }
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPWeightValidatorOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        String message = Utilities.getStringProperty(node, "message");
        return new DBSPWeightValidatorOperator(CalciteEmptyRel.INSTANCE, info.getInput(0), message)
                .addAnnotations(info.annotations(), DBSPWeightValidatorOperator.class);
    }
}
