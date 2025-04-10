package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

/** An item holding a comparator */
public class DBSPComparatorItem extends DBSPItem {
    public final DBSPComparatorExpression expression;

    public DBSPComparatorItem(DBSPComparatorExpression expression) {
        super(expression.getNode());
        this.expression = expression;
    }

    @Override
    public String getName() {
        return this.expression.getComparatorStructName();
    }

    @Override
    public DBSPStatement deepCopy() {
        return new DBSPComparatorItem(this.expression);
    }

    @Override
    public EquivalenceResult equivalent(EquivalenceContext context, DBSPStatement other) {
        // Two different comparator items are never equivalent
        return new EquivalenceResult(false, context);
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
        DBSPComparatorItem o = other.as(DBSPComparatorItem.class);
        if (o == null)
            return false;
        return this.expression == o.expression;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.getName());
    }

    @Override
    public DBSPType getType() {
        return this.expression.getType();
    }
}
