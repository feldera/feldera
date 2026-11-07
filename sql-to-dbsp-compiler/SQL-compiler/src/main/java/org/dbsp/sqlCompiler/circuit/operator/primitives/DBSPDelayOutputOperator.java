package org.dbsp.sqlCompiler.circuit.operator.primitives;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IPrimitive;
import org.dbsp.sqlCompiler.circuit.operator.ISplitOutput;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
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

/** The output half of a z^-1 operator that closes a cycle: it produces the delayed value.
 * It has no inputs.  The value comes from the input half {@link DBSPDelayInputOperator} whose
 * {@link DBSPDelayInputOperator#output} field refers to this operator.
 * If the function is specified, it is the initial value produced by the delay. */
public final class DBSPDelayOutputOperator extends DBSPSimpleOperator implements ISplitOutput, IPrimitive {
    /** The kind of the stream that the delay carries. */
    public final StreamKind kind;

    public DBSPDelayOutputOperator(CalciteRelNode node, @Nullable DBSPExpression initial,
                                   DBSPType outputType, StreamKind kind) {
        super(node, initial == null ? "transaction_delay_output" : "transaction_delay_output_with_initial_value",
                initial, outputType);
        if (initial != null && !initial.getType().sameType(outputType))
            throw new InternalCompilerError("Delay output has type " + outputType +
                    " but initial value has type " + initial.getType());
        this.kind = kind;
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
            return new DBSPDelayOutputOperator(this.getRelNode(), function, outputType, this.kind)
                    .copyAnnotations(this);
        return this;
    }

    /** Each instance is the end of a different back-edge. */
    @Override
    public boolean equivalent(DBSPOperator other) {
        return this == other;
    }

    @Override
    protected StreamKind computeOutputKind() {
        return this.kind;
    }

    @SuppressWarnings("unused")
    public static DBSPDelayOutputOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        StreamKind kind = StreamKind.valueOf(Utilities.getStringProperty(node, "kind"));
        return new DBSPDelayOutputOperator(CalciteEmptyRel.INSTANCE, info.function(), info.outputType(), kind)
                .addAnnotations(info.annotations(), DBSPDelayOutputOperator.class);
    }
}
