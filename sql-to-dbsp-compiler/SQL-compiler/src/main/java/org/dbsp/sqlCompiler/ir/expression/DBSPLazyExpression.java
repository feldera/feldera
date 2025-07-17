package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeLazy;
import org.dbsp.util.IIndentStream;

/** Represents an expression that is lazily evaluated. */
public class DBSPLazyExpression extends DBSPExpression {
    public final DBSPExpression expression;

    public static final String RUST_CRATE = "std::cell::LazyCell";
    public static final String RUST_IMPLEMENTATION = "LazyCell";

    public DBSPLazyExpression(DBSPExpression expression) {
        super(expression.getNode(), new DBSPTypeLazy(expression.getType()));
        this.expression = expression;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPLazyExpression(this.expression.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPLazyExpression otherExpression = other.as(DBSPLazyExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.expression, otherExpression.expression);
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
        DBSPLazyExpression otherExpression = other.as(DBSPLazyExpression.class);
        if (otherExpression == null)
            return false;
        return this.expression == otherExpression.expression;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("LazyCell::new(|| ")
                .append(this.expression)
                .append(")");
    }

    @SuppressWarnings("unused")
    public static DBSPLazyExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression expression = fromJsonInner(node, "expression", decoder, DBSPExpression.class);
        return new DBSPLazyExpression(expression);
    }
}
