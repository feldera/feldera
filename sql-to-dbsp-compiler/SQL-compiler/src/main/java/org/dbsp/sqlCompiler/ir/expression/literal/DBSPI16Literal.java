package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPI16Literal extends DBSPLiteral {
    @Nullable
    public final Short value;

    public DBSPI16Literal() {
        this(null, true);
    }

    public DBSPI16Literal(short value) {
        this(value, false);
    }

    public DBSPI16Literal(CalciteObject node, DBSPType type , @Nullable Short value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPI16Literal(@Nullable Short value, boolean nullable) {
        this(CalciteObject.EMPTY, DBSPTypeInteger.SIGNED_16.setMayBeNull(nullable), value);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getNonNullable() {
        return new DBSPI16Literal(Objects.requireNonNull(this.value));
    }

    public DBSPTypeInteger getIntegerType() {
        return this.type.to(DBSPTypeInteger.class);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPI16Literal that = (DBSPI16Literal) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")null");
        else
            return builder.append(this.value.toString());
    }
}
