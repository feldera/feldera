package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** Represents an expression of the form Some(e).
 * Does not really support arbitrary expressions for e:
 * they must be non-nullable. */
public final class DBSPSomeExpression extends DBSPExpression {
    public final DBSPExpression expression;

    public DBSPSomeExpression(CalciteObject node, DBSPExpression expression) {
        super(node, expression.getType().withMayBeNull(true));
        Utilities.enforce(!expression.getType().mayBeNull);
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
        DBSPSomeExpression o = other.as(DBSPSomeExpression.class);
        if (o == null)
            return false;
        return this.expression == o.expression;
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

    @SuppressWarnings("unused")
    public static DBSPSomeExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression expression = fromJsonInner(node, "expression", decoder, DBSPExpression.class);
        return new DBSPSomeExpression(CalciteObject.EMPTY, expression);
    }
}
