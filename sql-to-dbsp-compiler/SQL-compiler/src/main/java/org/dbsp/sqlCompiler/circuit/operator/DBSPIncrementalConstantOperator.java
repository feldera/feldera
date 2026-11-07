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
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** A {@link DBSPConstantOperator} followed by a {@link DBSPDifferentiateOperator}:
 * the value at the first step, and no changes after the first step.
 * The function field holds the value.  The operator keeps state: it records that it has
 * produced the value. */
public final class DBSPIncrementalConstantOperator extends DBSPSimpleOperator implements IStateful {
    public DBSPIncrementalConstantOperator(CalciteRelNode node, DBSPExpression value) {
        super(node, "incremental_constant", value, value.getType());
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
        Utilities.enforce(newInputs.isEmpty());
        if (this.mustReplace(force, function, newInputs, outputType))
            return new DBSPIncrementalConstantOperator(this.getRelNode(), Objects.requireNonNull(function))
                    .copyAnnotations(this);
        return this;
    }

    @Override
    protected StreamKind computeOutputKind() {
        return StreamKind.DELTA;
    }

    @SuppressWarnings("unused")
    public static DBSPIncrementalConstantOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPIncrementalConstantOperator(CalciteEmptyRel.INSTANCE, info.getFunction())
                .addAnnotations(info.annotations(), DBSPIncrementalConstantOperator.class);
    }
}
