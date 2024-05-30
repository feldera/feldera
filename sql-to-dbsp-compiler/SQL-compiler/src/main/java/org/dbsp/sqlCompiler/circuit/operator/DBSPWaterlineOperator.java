package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Given a stream, it computes max(function(stream), delay(this)). */
public final class DBSPWaterlineOperator extends DBSPUnaryOperator {
    /** Initial value of waterline */
    public final DBSPExpression init;

    public DBSPWaterlineOperator(CalciteObject node, DBSPExpression init,
                                 DBSPClosureExpression function, DBSPOperator input) {
        super(node, "waterline_monotonic", function, function.getResultType(), false, input);
        this.init = init;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPWaterlineOperator(this.getNode(), this.init,
                Objects.requireNonNull(expression).to(DBSPClosureExpression.class),
                this.input());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWaterlineOperator(this.getNode(), this.init,
                    this.getFunction().to(DBSPClosureExpression.class), newInputs.get(0));
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
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPWaterlineOperator otherOperator = other.as(DBSPWaterlineOperator.class);
        if (otherOperator == null)
            return false;
        return this.init.equivalent(otherOperator.init);
    }
}
