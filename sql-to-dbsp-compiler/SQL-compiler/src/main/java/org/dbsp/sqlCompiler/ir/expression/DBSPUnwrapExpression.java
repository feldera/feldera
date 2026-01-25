package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import java.util.Objects;

/** Represents an unwrap() method call in Rust, as applied to
 * an expression with a nullable type.
 * This is created by calling 'Expression.unwrap'.
 * Note that there is a different resultUnwrap, which is meant
 * to be applied to Result values.
 * An unwrap "neverFails" if the context promises that it can never fail.
 * E.g., if (!x.is_none()) x.unwrap();
 * Such unwraps have different optimization rules from "standard" unwraps.
 * The unwrap operations cannot be removed -- they still convert Option[T] to T, and generate the same code.*/
public final class DBSPUnwrapExpression extends DBSPExpression {
    /** An empty message indicates an unwrap which never fails -- we expect this message can never be displayed. */
    public final String message;
    public final DBSPExpression expression;

    public DBSPUnwrapExpression(String message, DBSPExpression expression) {
        super(expression.getNode(), expression.getType().withMayBeNull(false));
        this.message = message;
        this.expression = expression;
        Utilities.enforce(expression.getType().mayBeNull);
    }

    /** Guaranteed by construction to never fail */
    public boolean neverFails() {
        return this.message.isEmpty();
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
        return Objects.equals(this.message, o.message) && this.expression == o.expression;
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

    public DBSPExpression simplify() {
        if (this.expression.is(DBSPSomeExpression.class)) {
            return this.expression.to(DBSPSomeExpression.class).expression;
        }
        if (this.expression.isNone()) {
            return new DBSPFailExpression(this.getNode(), this.type, this.message);
        }
        if (this.expression.is(DBSPFailExpression.class)) {
            return new DBSPFailExpression(this.getNode(), this.type, this.message);
        }
        DBSPBaseTupleExpression tuple = this.expression.as(DBSPBaseTupleExpression.class);
        if (tuple != null) {
            // null case handled by isNone above
            Utilities.enforce(tuple.fields != null);
            // Convert to non-nullable tuple constructor
            switch (tuple.getType().code) {
                case RAW_TUPLE:
                    return new DBSPRawTupleExpression(tuple.fields);
                case TUPLE:
                    return new DBSPTupleExpression(tuple.fields);
                // Otherwise this compiles into an None.unwrap().
                default:
                    break;
            }
        }
        return this;
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