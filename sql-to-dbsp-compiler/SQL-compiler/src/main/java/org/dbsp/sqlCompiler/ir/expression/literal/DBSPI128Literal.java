package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

public final class DBSPI128Literal extends DBSPIntLiteral implements IsNumericLiteral {
    @Nullable
    public final BigInteger value;

    public DBSPI128Literal() {
        this((BigInteger)null, true);
    }

    public DBSPI128Literal(BigInteger value) {
        this(value, false);
    }

    public DBSPI128Literal(int value) {
        this(BigInteger.valueOf(value), false);
    }

    public DBSPI128Literal(CalciteObject node, DBSPType type , @Nullable BigInteger value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPI128Literal(CalciteObject node, @Nullable BigInteger value, boolean nullable) {
        this(node, new DBSPTypeInteger(CalciteObject.EMPTY, 128, true, nullable), value);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
    }

    public DBSPI128Literal(@Nullable BigInteger value, boolean nullable) {
        this(CalciteObject.EMPTY, value, nullable);
    }

    public DBSPI128Literal(@Nullable Integer value, boolean nullable) {
        this(CalciteObject.EMPTY, value == null ? null : BigInteger.valueOf(value), nullable);
    }

    @Override
    public boolean gt0() {
        assert this.value != null;
        return this.value.compareTo(BigInteger.ZERO) > 0;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPI128Literal(this.getNode(), this.type, this.value);
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
        if (o == null || getClass() != o.getClass()) return false;
        DBSPI128Literal that = (DBSPI128Literal) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPI128Literal(this.checkIfNull(this.value, mayBeNull), mayBeNull);
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

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }
}
