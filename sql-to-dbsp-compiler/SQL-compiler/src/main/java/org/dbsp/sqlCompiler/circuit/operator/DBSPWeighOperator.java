package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import java.util.List;

@NonCoreIR
public final class DBSPWeighOperator extends DBSPUnaryOperator {
    static DBSPTypeZSet outputType(DBSPTypeIndexedZSet sourceType) {
        return new DBSPTypeZSet(sourceType.elementType);
    }

    public DBSPWeighOperator(CalciteObject node, DBSPExpression function, DBSPOperator source) {
        super(node, "weigh", function,
                outputType(source.getOutputIndexedZSetType()), false, source);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWeighOperator(this.getNode(), this.getFunction(), newInputs.get(0));
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
