package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

/** This operator operates only on IndexedZSets.
 * It contains an integrator inside.  It takes a positive update
 * to the indexed collection and produces a corresponding retraction
 * for the pre-existing key. */
@NonCoreIR
public final class DBSPUpsertFeedbackOperator extends DBSPUnaryOperator {
    public DBSPUpsertFeedbackOperator(CalciteRelNode node, OutputPort source) {
        super(node, "upsert_feedback", null, source.outputType(), source.isMultiset(), source);
        source.getOutputIndexedZSetType();  // asserts that the type is right
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
            return new DBSPUpsertFeedbackOperator(
                    this.getRelNode(), newInputs.get(0)).copyAnnotations(this);
        }
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPUpsertFeedbackOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPUpsertFeedbackOperator(CalciteEmptyRel.INSTANCE, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPUpsertFeedbackOperator.class);
    }
}
