package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.util.IIndentStream;

/** Represents an expression of the form Some(e). */
@NonCoreIR
public final class DBSPSomeExpression extends DBSPExpression {
    public final DBSPExpression expression;

    public DBSPSomeExpression(CalciteObject node, DBSPExpression expression) {
        super(node, expression.getType().setMayBeNull(true));
        this.expression = expression;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }


    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPSomeExpression o = other.as(DBSPSomeExpression.class);
        if (o == null)
            return false;
        return this.expression == o.expression &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("Some(")
                .append(this.expression)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPSomeExpression(this.getNode(), this.expression.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPSomeExpression otherExpression = other.as(DBSPSomeExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.expression, otherExpression.expression);
    }
}
