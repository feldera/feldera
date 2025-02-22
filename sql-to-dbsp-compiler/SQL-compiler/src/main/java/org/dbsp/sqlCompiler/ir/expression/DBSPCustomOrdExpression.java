package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;
import org.dbsp.util.IIndentStream;

/** An expression that carries a comparator for custom ordering */
public final class DBSPCustomOrdExpression extends DBSPExpression {
    public final DBSPExpression source;
    public final DBSPComparatorExpression comparator;

    public DBSPCustomOrdExpression(
            CalciteObject node, DBSPExpression source,
            DBSPComparatorExpression comparator) {
        super(node, new DBSPTypeWithCustomOrd(node, source.getType()));
        this.source = source;
        this.comparator = comparator;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("source");
        this.source.accept(visitor);
        visitor.property("comparator");
        this.comparator.accept(visitor);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPCustomOrdExpression o = other.as(DBSPCustomOrdExpression.class);
        if (o == null)
            return false;
        return this.source == o.source &&
                this.comparator == o.comparator;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("WithCustomOrd(")
                .append(this.source)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPCustomOrdExpression(
                this.getNode(), this.source.deepCopy(),
                this.comparator.deepCopy().to(DBSPComparatorExpression.class));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPCustomOrdExpression otherExpression = other.as(DBSPCustomOrdExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.source, otherExpression.source) &&
                context.equivalent(this.comparator, otherExpression.comparator);
    }
}
