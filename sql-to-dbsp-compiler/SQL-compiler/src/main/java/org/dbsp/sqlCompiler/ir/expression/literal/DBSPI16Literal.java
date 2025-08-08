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

public final class DBSPI16Literal extends DBSPIntLiteral implements IsNumericLiteral {
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
        this(CalciteObject.EMPTY, DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT16, nullable), value);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPI16Literal(this.getNode(), this.type, this.value);
    }

    @Override
    public int compare(IsNumericLiteral other) {
        DBSPI16Literal oi = other.to(DBSPI16Literal.class);
        Utilities.enforce(this.value != null);
        Utilities.enforce(oi.value != null);
        return this.value.compareTo(oi.value);
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
    public IsNumericLiteral negate() {
        if (this.value == null)
            return this;
        if (this.value == Short.MIN_VALUE) {
            throw new ArithmeticException("Negate Short.MIN_VALUE");
        }
        return new DBSPI16Literal(this.getNode(), this.type, (short)-this.value);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPI16Literal(this.checkIfNull(this.value, mayBeNull), mayBeNull);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
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

    @Override
    public boolean gt0() {
        Utilities.enforce(this.value != null);
        return this.value > 0;
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
        if (this.value == null)
            return null;
        return BigInteger.valueOf(this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPI16Literal fromJson(JsonNode node, JsonDecoder decoder) {
        Short value = null;
        if (node.has("value"))
            value = (short) Utilities.getIntProperty(node, "value");
        DBSPType type = getJsonType(node, decoder);
        return new DBSPI16Literal(CalciteObject.EMPTY, type, value);
    }
}
