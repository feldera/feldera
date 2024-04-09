package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.Objects;

/** Representation of an expression and its monotonicity */
public class MonotoneExpression {
    static long crtId = 0;

    final long id;
    /** Original expression which is represented */
    final DBSPExpression expression;
    /** Monotonicity of the expression represented as a type */
    final IMaybeMonotoneType type;
    /** An expression that contains only parts of the original expression
     * but which contains exactly the monotone parts. */
    @Nullable final DBSPExpression reducedExpression;

    public MonotoneExpression(DBSPExpression expression, IMaybeMonotoneType type,
                              @Nullable DBSPExpression reducedExpression) {
        this.expression = expression;
        this.type = type;
        this.reducedExpression = reducedExpression;
        this.id = crtId++;
        DBSPType expressionType = expression.getType();
        DBSPType monotoneType = type.getType();
        assert expressionType.sameType(monotoneType):
            "Types differ " + expressionType + " and " + monotoneType;
    }

    public DBSPExpression getReducedExpression() {
        return Objects.requireNonNull(this.reducedExpression);
    }

    public IMaybeMonotoneType getMonotoneType() {
        return this.type;
    }

    public boolean mayBeMonotone() {
        return this.getMonotoneType().mayBeMonotone();
    }

    public IMaybeMonotoneType copyMonotonicity(DBSPType type) {
        return this.getMonotoneType().copyMonotonicity(type);
    }

    @Override
    public String toString() {
        return "ME(" + this.id + "): " + this.expression + ": " + this.type + " /" +
                this.reducedExpression + ": " +
                (this.reducedExpression != null ? this.reducedExpression.getType() : "");
    }
}
