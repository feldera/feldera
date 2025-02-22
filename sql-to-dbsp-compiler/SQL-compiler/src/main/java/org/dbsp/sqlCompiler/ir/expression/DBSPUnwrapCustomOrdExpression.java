package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;
import org.dbsp.util.IIndentStream;

/** Unpacks the data from a {@link DBSPCustomOrdExpression}.
 * The result has an ordinary tuple type. */
public final class DBSPUnwrapCustomOrdExpression extends DBSPExpression {
    public final DBSPExpression expression;

    public DBSPUnwrapCustomOrdExpression(DBSPExpression expression) {
        super(expression.getNode(), expression.getType().to(DBSPTypeWithCustomOrd.class).getDataType());
        this.expression = expression;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("expression");
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPUnwrapCustomOrdExpression o = other.as(DBSPUnwrapCustomOrdExpression.class);
        if (o == null)
            return false;
        return this.expression == o.expression;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPUnwrapCustomOrdExpression(this.expression.deepCopy());
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.expression)
                .append(".get()");
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPUnwrapCustomOrdExpression otherExpression = other.as(DBSPUnwrapCustomOrdExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.expression, otherExpression.expression);
    }
}
