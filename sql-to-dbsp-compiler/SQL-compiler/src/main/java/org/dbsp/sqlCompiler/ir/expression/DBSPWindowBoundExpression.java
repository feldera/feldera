package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IIndentStream;

/** An expression that represents a window bound.
 * This is only used in DBSPPartitionedRollingAggregateOperator
 * and related operators. */
public class DBSPWindowBoundExpression extends DBSPExpression {
    public final boolean isPreceding;
    public final DBSPExpression representation;

    public DBSPWindowBoundExpression(
            CalciteObject object, boolean isPreceding, DBSPExpression representation) {
        super(object, representation.getType());
        this.isPreceding = isPreceding;
        this.representation = representation;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPWindowBoundExpression(
                this.getNode(), this.isPreceding, this.representation.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPWindowBoundExpression otherExpression = other.as(DBSPWindowBoundExpression.class);
        if (otherExpression == null)
            return false;
        return this.isPreceding == otherExpression.isPreceding &&
                context.equivalent(this.representation, otherExpression.representation);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("representation");
        this.representation.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPWindowBoundExpression o = other.as(DBSPWindowBoundExpression.class);
        if (o == null)
            return false;
        return this.isPreceding == o.isPreceding &&
                this.representation == o.representation;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("RelOffset::")
                .append(this.isPreceding ? "Before" : "After")
                .append("(")
                .append(this.representation)
                .append(")");
    }
}
