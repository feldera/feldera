package org.dbsp.sqlCompiler.ir.expression.literal;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

public final class DBSPI128Literal extends DBSPIntLiteral implements IsNumericLiteral {
    static final BigInteger MIN = BigInteger.ONE.shiftLeft(127).negate();
    static final BigInteger MAX = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);

    @Nullable
    public final BigInteger value;

    public DBSPI128Literal() {
        this((BigInteger)null, true);
    }

    public DBSPI128Literal(BigInteger value) {
        this(value, false);
    }

    public DBSPI128Literal(CalciteObject node, DBSPType type , @Nullable BigInteger value) {
        super(node, type, value == null);
        this.value = value;
        if (value != null) {
            assert value.compareTo(MAX) <= 0;
            assert value.compareTo(MIN) >= 0;
        }
    }

    public DBSPI128Literal(CalciteObject node, @Nullable BigInteger value, boolean nullable) {
        this(node, DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT128, nullable), value);
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
    public IsNumericLiteral negate() {
        if (this.value == null)
            return this;
        if (this.value.compareTo(MIN) == 0)
            throw new ArithmeticException("Negate i128 overflow");
        return new DBSPI128Literal(this.getNode(), this.type, this.value.negate());
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
    public boolean sameValue(@Nullable ISameValue o) {
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
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        return this.value.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Override @Nullable
    public BigInteger getValue() {
        return this.value;
    }

    @SuppressWarnings("unused")
    public static DBSPI128Literal fromJson(JsonNode node, JsonDecoder decoder) {
        BigInteger value = null;
        if (node.has("value"))
            value = new BigInteger(Utilities.getStringProperty(node, "value"));
        DBSPType type = getJsonType(node, decoder);
        return new DBSPI128Literal(CalciteObject.EMPTY, type, value);
    }
}
