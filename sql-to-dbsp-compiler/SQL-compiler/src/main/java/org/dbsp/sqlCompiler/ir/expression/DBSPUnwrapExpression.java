package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** Represents an unwrap() method call in Rust, as applied to
 * an expression with a nullable type.
 * This is created by calling 'Expression.unwrap'.
 * Note that there is a different resultUnwrap, which is meant
 * to be applied to Result values */
public final class DBSPUnwrapExpression extends DBSPExpression {
    public final String message;
    public final DBSPExpression expression;

    public DBSPUnwrapExpression(String message, DBSPExpression expression) {
        super(expression.getNode(), expression.getType().withMayBeNull(false));
        this.message = message;
        this.expression = expression;
        Utilities.enforce(expression.getType().mayBeNull);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.property("expression");
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPUnwrapExpression o = other.as(DBSPUnwrapExpression.class);
        if (o == null)
            return false;
        return this.message.equals(o.message) && this.expression == o.expression;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPUnwrapExpression(this.message, this.expression.deepCopy());
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.message.isEmpty())
            return builder.append(this.expression)
                .append(".unwrap()");
        else
            return builder.append(this.expression)
                    .append(".expect(")
                    .append(Utilities.doubleQuote(this.message, false))
                    .append(")");
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPUnwrapExpression otherExpression = other.as(DBSPUnwrapExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.expression, otherExpression.expression);
    }

    @SuppressWarnings("unused")
    public static DBSPUnwrapExpression fromJson(JsonNode node, JsonDecoder decoder) {
        getJsonType(node, decoder);
        DBSPExpression expression = fromJsonInner(node, "expression", decoder, DBSPExpression.class);
        String message = Utilities.getStringProperty(node, "message");
        return new DBSPUnwrapExpression(message, expression);
    }
}