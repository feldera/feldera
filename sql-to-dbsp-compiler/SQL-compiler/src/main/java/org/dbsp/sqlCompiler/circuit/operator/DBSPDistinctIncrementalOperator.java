package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

@NonCoreIR
public final class DBSPDistinctIncrementalOperator extends DBSPBinaryOperator {
    // In the DBSP paper this operator was called H
    public DBSPDistinctIncrementalOperator(CalciteObject node, DBSPOperator integral, DBSPOperator delta) {
        super(node, "distinct_incremental", null, delta.outputType, false, integral, delta);
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
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return this;
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2;
        if (force || this.inputsDiffer(newInputs))
            return new DBSPDistinctIncrementalOperator(
                    this.getNode(), newInputs.get(0), newInputs.get(1));
        return this;
    }
}
