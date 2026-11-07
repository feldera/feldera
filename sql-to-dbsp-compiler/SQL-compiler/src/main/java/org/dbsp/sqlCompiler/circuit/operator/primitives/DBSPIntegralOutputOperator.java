package org.dbsp.sqlCompiler.circuit.operator.primitives;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IPrimitive;
import org.dbsp.sqlCompiler.circuit.operator.ISplitOutput;
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

/** The output half of the integral of a stream that closes a cycle: it produces the integral
 * as it was before the step.  It has no inputs.  The stream comes from the input half
 * {@link DBSPIntegralInputOperator} whose {@link DBSPIntegralInputOperator#output} field refers
 * to this operator. */
public final class DBSPIntegralOutputOperator extends DBSPSimpleOperator implements ISplitOutput, IPrimitive {
    public DBSPIntegralOutputOperator(CalciteRelNode node, DBSPType outputType) {
        super(node, "integral_output", null, outputType);
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
        Utilities.enforce(function == null);
        Utilities.enforce(newInputs.isEmpty());
        if (this.mustReplace(force, null, newInputs, outputType))
            return new DBSPIntegralOutputOperator(this.getRelNode(), outputType).copyAnnotations(this);
        return this;
    }

    /** Each instance is the end of a different back-edge. */
    @Override
    public boolean equivalent(DBSPOperator other) {
        return this == other;
    }

    @Override
    protected StreamKind computeOutputKind() {
        return StreamKind.COLLECTION_SNAPSHOT;
    }

    @SuppressWarnings("unused")
    public static DBSPIntegralOutputOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPIntegralOutputOperator(CalciteEmptyRel.INSTANCE, info.outputType())
                .addAnnotations(info.annotations(), DBSPIntegralOutputOperator.class);
    }
}
