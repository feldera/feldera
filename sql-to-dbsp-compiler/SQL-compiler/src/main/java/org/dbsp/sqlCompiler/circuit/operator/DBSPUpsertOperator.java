package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

/** Primitive Upsert operator.
 * Used to implement the UpsertFeedback operator. */
@NonCoreIR
public final class DBSPUpsertOperator extends DBSPBinaryOperator {
    public DBSPUpsertOperator(CalciteObject node, DBSPOperator delta, DBSPOperator integral) {
        super(node, "primitive_upsert", null, delta.outputType, false, delta, integral);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return this;
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2 : "Expected 2 inputs";
        if (force || this.inputsDiffer(newInputs))
            return new DBSPUpsertOperator(this.getNode(), newInputs.get(0), newInputs.get(1));
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
}
