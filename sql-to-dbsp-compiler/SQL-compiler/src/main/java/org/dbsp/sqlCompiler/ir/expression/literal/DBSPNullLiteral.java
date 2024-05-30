package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/** A literal with type NULL, the only value of this type. */
public final class DBSPNullLiteral extends DBSPLiteral {
    public DBSPNullLiteral(CalciteObject node, DBSPType type, @Nullable Object value) {
        super(node, type, true);
        if (value != null)
            throw new InternalCompilerError("Value must be null", this);
        if (!this.getType().sameType(new DBSPTypeNull(CalciteObject.EMPTY)))
            throw new InternalCompilerError("Type must be NULL", this);
    }

    public DBSPNullLiteral() {
        this(CalciteObject.EMPTY, new DBSPTypeNull(CalciteObject.EMPTY), null);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
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
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("null");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPNullLiteral(this.getNode(), this.getType(), null);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        return other.is(DBSPNullLiteral.class);
    }
}