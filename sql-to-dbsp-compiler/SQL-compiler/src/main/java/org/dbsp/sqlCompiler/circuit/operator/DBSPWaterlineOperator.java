package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Given a stream, it computes max(function(stream), delay(this)).
 */
public class DBSPWaterlineOperator extends DBSPUnaryOperator {
    /** Initial value of waterline */
    public final DBSPExpression init;

    public DBSPWaterlineOperator(CalciteObject node, DBSPExpression init, DBSPExpression function, DBSPType
            outputType, DBSPOperator input) {
        super(node, "waterline_monotonic", function, outputType, false, input);
        this.init = init;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPWaterlineOperator(this.getNode(), this.init,
                Objects.requireNonNull(expression), this.outputType, this.input());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWaterlineOperator(this.getNode(), this.init,
                    this.getFunction(), this.outputType, newInputs.get(0));
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
