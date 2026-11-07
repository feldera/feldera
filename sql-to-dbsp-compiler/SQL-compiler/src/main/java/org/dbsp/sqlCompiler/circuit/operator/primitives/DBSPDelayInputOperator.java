package org.dbsp.sqlCompiler.circuit.operator.primitives;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.ILinear;
import org.dbsp.sqlCompiler.circuit.operator.IPrimitive;
import org.dbsp.sqlCompiler.circuit.operator.ISplitInput;
import org.dbsp.sqlCompiler.circuit.operator.ISplitOutput;
import org.dbsp.sqlCompiler.circuit.operator.IStateful;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** The input half of a z^-1 operator that closes a cycle: it reads the value to delay.
 * It has no consumers; its output half {@link #output} produces the delayed value. */
public final class DBSPDelayInputOperator extends DBSPUnaryOperator
        implements ISplitInput, IPrimitive, IStateful, ILinear {
    public final DBSPDelayOutputOperator output;

    public DBSPDelayInputOperator(CalciteRelNode node, OutputPort source, DBSPDelayOutputOperator output) {
        super(node, "transaction_delay_input", null, source.outputType(), source);
        this.output = output;
    }

    /** A delay carries a stream of any kind unchanged; {@link #output} produces it. */
    @Override
    protected StreamKind computeOutputKind() {
        StreamKind kind = this.input().kind();
        Utilities.enforce(this.output.outputType.sameType(this.outputType) && this.output.kind == kind,
                () -> "Delay input " + this + " receives " + kind + " of type " + this.outputType +
                        " but its output " + this.output + " produces " + this.output.kind +
                        " of type " + this.output.outputType);
        return kind;
    }

    @Override
    public ISplitOutput splitOutput() {
        return this.output;
    }

    @Override
    public DBSPSimpleOperator withSplitOutput(ISplitOutput output) {
        return new DBSPDelayInputOperator(this.getRelNode(), this.input(),
                output.outputPort().node().to(DBSPDelayOutputOperator.class))
                .copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        Utilities.enforce(function == null);
        if (this.mustReplace(force, null, newInputs, outputType))
            return new DBSPDelayInputOperator(this.getRelNode(), newInputs.get(0), this.output)
                    .copyAnnotations(this);
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

    /** True if {@code other} delays the same stream and its output starts with the same value. */
    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPDelayInputOperator otherOperator = other.as(DBSPDelayInputOperator.class);
        return otherOperator != null
                && this.output.kind == otherOperator.output.kind
                && EquivalenceContext.equiv(this.output.function, otherOperator.output.function);
    }

    @SuppressWarnings("unused")
    public static DBSPDelayInputOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPDelayOutputOperator output = OutputPort.fromJson(Utilities.getProperty(node, "splitOutput"), decoder)
                .node().to(DBSPDelayOutputOperator.class);
        return new DBSPDelayInputOperator(CalciteEmptyRel.INSTANCE, info.getInput(0), output)
                .addAnnotations(info.annotations(), DBSPDelayInputOperator.class);
    }
}
