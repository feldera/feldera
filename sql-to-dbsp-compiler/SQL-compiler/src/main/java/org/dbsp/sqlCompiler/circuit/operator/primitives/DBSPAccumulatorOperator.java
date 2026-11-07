package org.dbsp.sqlCompiler.circuit.operator.primitives;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Annotations;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.IMultiOutput;
import org.dbsp.sqlCompiler.circuit.operator.IPrimitive;
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

/** Gathers the changes of one step into one spine.
 * Output 0 is that spine; output 1 is a read-only snapshot of it.
 * Both outputs have the type of the input. */
public final class DBSPAccumulatorOperator extends DBSPOperator implements IMultiOutput, IPrimitive {
    public static final String OPERATION = "accumulate";
    /** The type of the input and of both outputs. */
    public final DBSPType outputType;

    public DBSPAccumulatorOperator(CalciteRelNode node, OutputPort input) {
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
        this.requireAllInputsAmong(StreamKind.DELTA);
        return switch (outputNo) {
            case 0 -> StreamKind.DELTA_SPINE;
            case 1 -> StreamKind.DELTA_SNAPSHOT;
            default -> throw new InternalCompilerError("No output " + outputNo, this);
        };
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        return other.is(DBSPAccumulatorOperator.class) && this.sameInputs(other);
    }

    @Override
    public DBSPOperator withInputs(List<OutputPort> newInputs, boolean force) {
        Utilities.enforce(newInputs.size() == 1);
        if (force || this.inputsDiffer(newInputs))
            return new DBSPAccumulatorOperator(this.getRelNode(), newInputs.get(0)).copyAnnotations(this);
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
    public static DBSPAccumulatorOperator fromJson(JsonNode node, JsonDecoder decoder) {
        List<OutputPort> inputs = Linq.list(Linq.map(
                Utilities.getProperty(node, "inputs").elements(), e -> OutputPort.fromJson(e, decoder)));
        Utilities.enforce(inputs.size() == 1);
        return new DBSPAccumulatorOperator(CalciteEmptyRel.INSTANCE, inputs.get(0))
                .addAnnotations(Annotations.fromJson(Utilities.getProperty(node, "annotations")),
                        DBSPAccumulatorOperator.class);
    }
}
