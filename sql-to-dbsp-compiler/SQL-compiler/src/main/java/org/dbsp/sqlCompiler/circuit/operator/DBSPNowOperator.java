package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;

import javax.annotation.Nullable;
import java.util.List;

/** Operator that generates the NOW() timestamp */
public final class DBSPNowOperator extends DBSPSimpleOperator {
    // zset!(Tup1::new(now()))
    static DBSPExpression createFunction(CalciteObject node) {
        return new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPApplyExpression(
                        "now", new DBSPTypeTimestamp(node, false))));
    }

    public DBSPNowOperator(CalciteObject node) {
        super(node, "now", createFunction(node),
                createFunction(node).getType(),
                false);
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
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return this;
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        return this;
    }
}
