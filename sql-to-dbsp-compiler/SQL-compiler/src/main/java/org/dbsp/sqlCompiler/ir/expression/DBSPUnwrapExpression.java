package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.IIndentStream;

/** Represents an unwrap() method call in Rust, as applied to
 * an expression with a nullable type.
 * This is created by calling 'Expression.unwrap'.
 * Note that there is a different resultUnwrap, which is meant
 * to be applied to Result values */
public final class DBSPUnwrapExpression extends DBSPExpression {
    public final DBSPExpression expression;

    public DBSPUnwrapExpression(DBSPExpression expression) {
        super(expression.getNode(), expression.getType().setMayBeNull(false));
        this.expression = expression;
        assert expression.getType().mayBeNull;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPUnwrapExpression o = other.as(DBSPUnwrapExpression.class);
        if (o == null)
            return false;
        return this.expression == o.expression &&
                this.hasSameType(o);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPUnwrapExpression(this.expression.deepCopy());
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.expression)
                .append(".unwrap()");
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPUnwrapExpression otherExpression = other.as(DBSPUnwrapExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.expression, otherExpression.expression);
    }
}
