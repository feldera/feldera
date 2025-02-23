package org.dbsp.sqlCompiler.ir.expression.literal;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/** A literal with type NULL, the only value of this type. */
public final class DBSPNullLiteral extends DBSPLiteral {
    DBSPNullLiteral() {
        super(CalciteObject.EMPTY, DBSPTypeNull.INSTANCE, true);
    }

    public static final DBSPNullLiteral INSTANCE = new DBSPNullLiteral();

    public static final String NULL = "NULL";

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        return o != null && getClass() == o.getClass();
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        if (!mayBeNull)
            throw new UnsupportedException(this.getNode());
        return this;
    }

    @Override
    public String toSqlString() {
        return NULL;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("null");
    }

    @Override
    public DBSPExpression deepCopy() {
        return this;
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        return other.is(DBSPNullLiteral.class);
    }

    @SuppressWarnings("unused")
    public static DBSPNullLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        return INSTANCE;
    }
}