package org.dbsp.sqlCompiler.circuit.operator.primitives;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.IPrimitive;
import org.dbsp.sqlCompiler.circuit.operator.ISplitInput;
import org.dbsp.sqlCompiler.circuit.operator.ISplitOutput;
import org.dbsp.sqlCompiler.circuit.operator.IStateful;
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

/** The input half of the integral of a stream that closes a cycle: it adds the changes of each
 * step to the integral.  It has no consumers; its output half {@link #output} produces the
 * integral. */
public final class DBSPIntegralInputOperator extends DBSPUnaryOperator
        implements ISplitInput, IPrimitive, IStateful {
    public final DBSPIntegralOutputOperator output;

    public DBSPIntegralInputOperator(CalciteRelNode node, OutputPort source, DBSPIntegralOutputOperator output) {
        super(node, "integral_input", null, source.outputType(), source);
        this.output = output;
    }

    @Override
    public ISplitOutput splitOutput() {
        return this.output;
    }

    @Override
    public DBSPSimpleOperator withSplitOutput(ISplitOutput output) {
        return new DBSPIntegralInputOperator(this.getRelNode(), this.input(),
                output.outputPort().node().to(DBSPIntegralOutputOperator.class))
                .copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        Utilities.enforce(function == null);
        if (this.mustReplace(force, null, newInputs, outputType))
            return new DBSPIntegralInputOperator(this.getRelNode(), newInputs.get(0), this.output)
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

    /** The kind of the integral that {@link #output} produces. */
    @Override
    protected StreamKind computeOutputKind() {
        this.requireAllInputsAmong(StreamKind.DELTA_SPINE);
        Utilities.enforce(this.output.outputType.sameType(this.outputType),
                () -> "Integral input " + this + " has type " + this.outputType +
                        " but its output " + this.output + " has type " + this.output.outputType);
        return this.output.outputKind(0);
    }

    @SuppressWarnings("unused")
    public static DBSPIntegralInputOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPIntegralOutputOperator output = OutputPort.fromJson(Utilities.getProperty(node, "splitOutput"), decoder)
                .node().to(DBSPIntegralOutputOperator.class);
        return new DBSPIntegralInputOperator(CalciteEmptyRel.INSTANCE, info.getInput(0), output)
                .addAnnotations(info.annotations(), DBSPIntegralInputOperator.class);
    }
}
