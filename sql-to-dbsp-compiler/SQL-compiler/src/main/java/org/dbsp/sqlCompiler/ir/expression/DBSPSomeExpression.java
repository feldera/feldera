package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.ir.InnerVisitor;

import javax.annotation.Nullable;

/**
 * Represents an expression of the form Some(e).
 */
public class DBSPSomeExpression extends DBSPExpression {
    public final DBSPExpression expression;

    public DBSPSomeExpression(@Nullable Object object, DBSPExpression expression) {
        super(object, expression.getNonVoidType().setMayBeNull(true));
        this.expression = expression;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this))
            return;
        this.expression.accept(visitor);
        visitor.postorder(this);
    }
}
