package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

public class DBSPVariantLiteral extends DBSPLiteral {
    // TODO: this may need to be an expression
    @Nullable public final DBSPLiteral value;
    // If true this VARIANT has the value NULL inside.
    // But it's a VARIANT object, not a NULL
    public final boolean isSqlNull;

    public DBSPVariantLiteral(@Nullable DBSPLiteral value, DBSPType type) {
        super(CalciteObject.EMPTY, type, value == null);
        this.value = value;
        this.isSqlNull = false;
        assert type.is(DBSPTypeVariant.class);
    }

    public DBSPVariantLiteral(boolean mayBeNull) {
        super(CalciteObject.EMPTY, new DBSPTypeVariant(CalciteObject.EMPTY, mayBeNull), false);
        this.isSqlNull = true;
        this.value = null;
    }

    public DBSPVariantLiteral(@Nullable DBSPLiteral value, boolean mayBeNull) {
        this(value, new DBSPTypeVariant(CalciteObject.EMPTY, mayBeNull));
    }

    public DBSPVariantLiteral(@Nullable DBSPLiteral value) {
        this(value, new DBSPTypeVariant(CalciteObject.EMPTY, false));
    }

    /** The value representing the SQL NULL */
    public static DBSPLiteral sqlNull(boolean mayBeNull) {
        return new DBSPVariantLiteral(mayBeNull);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        if (this.value != null)
            this.value.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPVariantLiteral that = (DBSPVariantLiteral) o;
        if (this.isSqlNull)
            return that.isSqlNull && this.value == that.value;
        if (this.value == null)
            return that.value == null;
        return this.value.sameValue(that.value);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPVariantLiteral(this.value, this.type.setMayBeNull(true));
    }

    @Override
    public String toSqlString() {
        return "VARIANT";
    }

    @Override
    public DBSPExpression deepCopy() {
        if (this.isSqlNull)
            return this;
        return new DBSPVariantLiteral(this.value, this.getType().mayBeNull);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("null");
        if (this.isSqlNull)
            return builder.append("VARIANT(NULL)");
        return builder.append("VARIANT(")
                .append(this.value)
                .append(")");
    }
}
