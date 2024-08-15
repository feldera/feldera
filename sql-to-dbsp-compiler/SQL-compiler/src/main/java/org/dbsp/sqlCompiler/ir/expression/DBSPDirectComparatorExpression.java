package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

/** A comparator that compares two values directly.
 * It takes a direction, indicating whether the sort is ascending or descending. */
public final class DBSPDirectComparatorExpression extends DBSPComparatorExpression {
    public final DBSPComparatorExpression source;
    public final boolean ascending;

    public DBSPDirectComparatorExpression(CalciteObject node, DBSPComparatorExpression source, boolean ascending) {
        super(node);
        this.source = source;
        this.ascending = ascending;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.source.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    public DBSPType comparedValueType() {
        return this.source.comparedValueType();
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPDirectComparatorExpression o = other.as(DBSPDirectComparatorExpression.class);
        if (o == null)
            return false;
        return this.source == o.source &&
                this.ascending == o.ascending &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.source)
                .append(".then_")
                .append(this.ascending ? "asc" : "desc")
                .append("(|t| t)");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPDirectComparatorExpression(
                this.getNode(), this.source.deepCopy().to(DBSPComparatorExpression.class),
                this.ascending);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPDirectComparatorExpression otherExpression = other.as(DBSPDirectComparatorExpression.class);
        if (otherExpression == null)
            return false;
        return this.ascending == otherExpression.ascending &&
                this.source.equivalent(context, otherExpression.source);
    }
}
