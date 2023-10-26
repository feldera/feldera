package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Lateness operator: represents the (incorrectly-named) watermark_monotonic operator
 * from DBSP.  Given a stream, it computes max(function(stream), delay(this)).
 */
public class DBSPLatenessOperator extends DBSPUnaryOperator {
    protected DBSPLatenessOperator(CalciteObject node, DBSPExpression function, DBSPType
            outputType, DBSPOperator input) {
        super(node, "watermark_monotonic", function, outputType, false, input);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPLatenessOperator(this.getNode(),
                Objects.requireNonNull(expression), this.outputType, this.input());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPLatenessOperator(this.getNode(), this.getFunction(), this.outputType, newInputs.get(0));
        return this;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.postorder(this);
    }
}
