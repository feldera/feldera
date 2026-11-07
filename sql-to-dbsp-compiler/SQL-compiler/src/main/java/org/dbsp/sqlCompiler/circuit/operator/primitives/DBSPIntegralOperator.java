package org.dbsp.sqlCompiler.circuit.operator.primitives;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Annotations;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.IMultiOutput;
import org.dbsp.sqlCompiler.circuit.operator.IPrimitive;
import org.dbsp.sqlCompiler.circuit.operator.IStateful;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.List;

/** Adds the spine of each step to the integral of the stream.
 * Output 0 is the integral before the step; output 1 is the integral after the step.
 * Both outputs are snapshots with the type of the input.
 * A back-edge cannot read output 0, because the circuit must stay acyclic: a back-edge uses
 * the split pair {@link DBSPIntegralOutputOperator} and {@link DBSPIntegralInputOperator}.
 * {@link DBSPIntegrateOperator} is the integral of a Z-set stream for the Rust backend. */
public final class DBSPIntegralOperator extends DBSPOperator implements IMultiOutput, IPrimitive, IStateful {
    public static final String OPERATION = "integral";
    /** The type of the input and of both outputs. */
    public final DBSPType outputType;

    public DBSPIntegralOperator(CalciteRelNode node, OutputPort input) {
        super(node);
        this.addInput(input);
        this.outputType = input.outputType();
    }

    @Override
    public boolean hasOutput(int outputNumber) {
        return outputNumber == 0 || outputNumber == 1;
    }

    @Override
    public int outputCount() {
        return 2;
    }

    @Override
    public DBSPType outputType(int outputNo) {
        if (!this.hasOutput(outputNo))
            throw new InternalCompilerError("No output " + outputNo, this);
        return this.outputType;
    }

    @Override
    public StreamKind outputKind(int outputNo) {
        this.requireAllInputsAmong(StreamKind.DELTA_SPINE);
        if (!this.hasOutput(outputNo))
            throw new InternalCompilerError("No output " + outputNo, this);
        return StreamKind.COLLECTION_SNAPSHOT;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        return other.is(DBSPIntegralOperator.class) && this.sameInputs(other);
    }

    @Override
    public DBSPOperator withInputs(List<OutputPort> newInputs, boolean force) {
        Utilities.enforce(newInputs.size() == 1);
        if (force || this.inputsDiffer(newInputs))
            return new DBSPIntegralOperator(this.getRelNode(), newInputs.get(0)).copyAnnotations(this);
        return this;
    }

    @Override
    public DBSPOperator asOperator() {
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
        visitor.property("outputType");
        this.outputType.accept(visitor);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("let ")
                .append(this.getNodeName(false))
                .append(" = ")
                .append(this.inputs.get(0).getOutputName())
                .append(".")
                .append(OPERATION)
                .append("();");
    }

    @SuppressWarnings("unused")
    public static DBSPIntegralOperator fromJson(JsonNode node, JsonDecoder decoder) {
        List<OutputPort> inputs = Linq.list(Linq.map(
                Utilities.getProperty(node, "inputs").elements(), e -> OutputPort.fromJson(e, decoder)));
        Utilities.enforce(inputs.size() == 1);
        return new DBSPIntegralOperator(CalciteEmptyRel.INSTANCE, inputs.get(0))
                .addAnnotations(Annotations.fromJson(Utilities.getProperty(node, "annotations")),
                        DBSPIntegralOperator.class);
    }
}
