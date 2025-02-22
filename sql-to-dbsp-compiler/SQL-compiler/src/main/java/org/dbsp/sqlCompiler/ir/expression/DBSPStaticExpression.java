package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IIndentStream;

/** Represents an expression that is compiled into a
 * LazyLock declaration and a reference to the lazy lock value.
 * The name of the cell is not represented explicitly. */
public class DBSPStaticExpression extends DBSPExpression {
    public final DBSPExpression initializer;

    public DBSPStaticExpression(CalciteObject node, DBSPExpression initializer) {
        super(node, initializer.getType());
        this.initializer = initializer;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPStaticExpression(this.getNode(), this.initializer.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        return this == other;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("initializer");
        this.initializer.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPStaticExpression o = other.as(DBSPStaticExpression.class);
        if (o == null)
            return false;
        return this.initializer == o.initializer;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return this.initializer.toString(builder);
    }

    public String getName() {
        return "STATIC" + this.getId();
    }
}
