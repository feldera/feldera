package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;

import java.util.List;

/**
 * This operator operates only on IndexedZSets.
 * It contains an integrator inside.  It takes a positive update
 * to the indexed collection and produces a corresponding retraction
 * for the pre-existing key.
 */
@NonCoreIR
public final class DBSPUpsertFeedbackOperator extends DBSPUnaryOperator {
    public DBSPUpsertFeedbackOperator(CalciteObject node, DBSPOperator source) {
        super(node, "upsert", null, source.outputType, source.isMultiset, source);
        source.getOutputIndexedZSetType();  // assert that the type is right
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
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPUpsertFeedbackOperator(
                    this.getNode(), newInputs.get(0));
        return this;
    }
}
