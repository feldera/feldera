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
    public final DBSPClosureExpression init;
    /** Function which extracts a timestamp */
    public final DBSPClosureExpression extractTs;

    public DBSPWaterlineOperator(CalciteObject node, DBSPClosureExpression init,
                                 DBSPClosureExpression extractTs,
                                 DBSPClosureExpression function, DBSPOperator input) {
        super(node, "waterline", function, function.getResultType(),
                false, input);
        this.init = init;
        this.extractTs = extractTs;
        assert init.parameters.length == 0;
        assert extractTs.parameters.length == 2;
        assert function.parameters.length == 2;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPWaterlineOperator(this.getNode(), this.init,
                this.extractTs,
                Objects.requireNonNull(expression).to(DBSPClosureExpression.class),
                this.input()).copyAnnotations(this);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWaterlineOperator(this.getNode(), this.init,
                    this.extractTs,
                    this.getClosureFunction(),
                    newInputs.get(0))
                    .copyAnnotations(this);
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
