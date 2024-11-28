package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

public class DBSPVariantLiteral extends DBSPLiteral {
    // this is usually a literal
    @Nullable public final DBSPExpression value;
    // If true this VARIANT has the value NULL inside.
    // But it's a VARIANT object, not a NULL
    public final boolean isSqlNull;

    public DBSPVariantLiteral(@Nullable DBSPExpression value, DBSPType type) {
        super(CalciteObject.EMPTY, type, value == null);
        this.value = value;
        this.isSqlNull = false;
        assert type.is(DBSPTypeVariant.class);
        assert value == null || value.is(ISameValue.class);
    }

    public DBSPVariantLiteral(boolean mayBeNull) {
        super(CalciteObject.EMPTY, new DBSPTypeVariant(CalciteObject.EMPTY, mayBeNull), false);
        this.isSqlNull = true;
        this.value = null;
    }

    public DBSPVariantLiteral(@Nullable DBSPExpression value, boolean mayBeNull) {
        this(value, new DBSPTypeVariant(CalciteObject.EMPTY, mayBeNull));
    }

    public DBSPVariantLiteral(@Nullable DBSPExpression value) {
        this(value, new DBSPTypeVariant(CalciteObject.EMPTY, false));
    }

    /** The value representing the SQL NULL */
    public static DBSPLiteral sqlNull(boolean mayBeNull) {
        return new DBSPVariantLiteral(mayBeNull);
    }

    @Override
    public boolean isConstant() {
        return this.isSqlNull || this.value == null || this.value.isConstantLiteral();
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
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPVariantLiteral that = (DBSPVariantLiteral) o;
        if (this.isSqlNull)
            return that.isSqlNull && this.value == that.value;
        if (this.value == null)
            return that.value == null;
        if (that.value == null)
            return false;
        return this.value.to(ISameValue.class).sameValue(that.value.to(ISameValue.class));
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPVariantLiteral(this.value, this.type.withMayBeNull(true));
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
