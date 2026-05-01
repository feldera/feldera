package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

/** Semantically this operator is similar to a {@link DBSPSumOperator}, but the implementation is very
 * different: it guarantees that successors will not see partial results within a transaction sub-step */
public final class DBSPAtomicSumOperator extends DBSPSimpleOperator implements ILinear {
    public DBSPAtomicSumOperator(CalciteRelNode node, List<OutputPort> inputs) {
        super(node, "atomic_sum", null, inputs.get(0).outputType(), true);
        for (OutputPort op: inputs) {
            this.addInput(op);
            if (!op.outputType().sameType(this.outputType)) {
                throw new InternalCompilerError("Sum operator input type " + op.outputType() +
                        " does not match output type " + this.outputType, this);
            }
        }
    }

    public DBSPAtomicSumOperator(CalciteRelNode node, OutputPort... inputs) {
        this(node, Linq.list(inputs));
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
            @Nullable DBSPExpression unused, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        boolean different = force;
        if (newInputs.size() != this.inputs.size())
            // Sum can have any number of inputs
            different = true;
        if (!different) {
            different = this.inputsDiffer(newInputs);
        }
        if (different)
            return new DBSPAtomicSumOperator(this.getRelNode(), newInputs).copyAnnotations(this);
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPAtomicSumOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPAtomicSumOperator(CalciteEmptyRel.INSTANCE, info.inputs())
                .addAnnotations(info.annotations(), DBSPAtomicSumOperator.class);
    }
}
