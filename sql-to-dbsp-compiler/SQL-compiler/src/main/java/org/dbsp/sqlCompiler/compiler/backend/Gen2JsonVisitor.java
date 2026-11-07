package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.ISplitInput;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPDelayInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPDelayOutputOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralOutputOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Serializes as JSON a circuit by writing operators represented as pairs of halves
 * as a single node each. */
public class Gen2JsonVisitor extends ToJsonOuterVisitor {
    /** Maps the output half of each split operator to its input half. */
    final Map<DBSPOperator, DBSPSimpleOperator> inputHalf;

    public Gen2JsonVisitor(DBSPCompiler compiler, int verbosity, ToJsonInnerVisitor innerVisitor) {
        super(compiler, verbosity, innerVisitor);
        this.inputHalf = new HashMap<>();
    }

    public static Gen2JsonVisitor create(DBSPCompiler compiler, int verbosity) {
        return new Gen2JsonVisitor(compiler, verbosity, createInner(compiler));
    }

    void findSplitOperators(Iterable<DBSPOperator> operators) {
        for (DBSPOperator operator: operators) {
            ISplitInput split = operator.as(ISplitInput.class);
            if (split != null)
                Utilities.putNew(this.inputHalf, split.splitOutput().outputPort().node(),
                        operator.to(DBSPSimpleOperator.class));
            DBSPNestedOperator nested = operator.as(DBSPNestedOperator.class);
            if (nested != null)
                this.findSplitOperators(nested.getAllOperators());
        }
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        this.inputHalf.clear();
        this.findSplitOperators(circuit.allOperators);
        return super.preorder(circuit);
    }

    /** A reference to 'port' that does not serialize its operator, which can come later. */
    void reference(OutputPort port) {
        this.stream.beginObject()
                .label("outputNumber").append(port.outputNumber)
                .label("operator").beginObject()
                .label("node").append(port.node().getId());
        this.stream.endObject();
        this.stream.endObject();
    }

    /** Writes 'output' and its input half as one node of class 'node'. */
    VisitDecision fused(DBSPSimpleOperator output, Class<? extends DBSPOperator> node) {
        if (this.checkDone(output, false))
            return VisitDecision.STOP;
        DBSPSimpleOperator input = Utilities.getExists(this.inputHalf, output);
        this.stream.beginObject()
                .label("class").append(node.getSimpleName());
        this.property("id");
        this.stream.append(output.getId());
        this.serialized.add(output.getId());
        this.label("annotations");
        output.annotations.asJson(this.stream);
        this.label("inputs");
        this.stream.beginArray();
        for (OutputPort port: input.inputs)
            this.reference(port);
        this.stream.endArray();
        output.accept(this.innerVisitor);
        this.stream.endObject();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntegralOutputOperator operator) {
        return this.fused(operator, DBSPIntegralOperator.class);
    }

    @Override
    public VisitDecision preorder(DBSPDelayOutputOperator operator) {
        return this.fused(operator, DBSPDelayOperator.class);
    }

    /** The node of the output half contains the input half */
    @Override
    public VisitDecision preorder(DBSPIntegralInputOperator operator) {
        return VisitDecision.STOP;
    }

    /** The node of the output half contains the input half */
    @Override
    public VisitDecision preorder(DBSPDelayInputOperator operator) {
        return VisitDecision.STOP;
    }
}
