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

public final class DBSPI8Literal extends DBSPIntLiteral implements IsNumericLiteral {
    @Nullable
    public final Byte value;

    public DBSPI8Literal() {
        this(null, true);
    }

    public DBSPI8Literal(Byte value) {
        this(value, false);
    }

    public DBSPI8Literal(CalciteObject node, DBSPType type, @Nullable Byte value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPI8Literal(@Nullable Byte value, boolean nullable) {
        this(CalciteObject.EMPTY, DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT8, nullable), value);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
    }

    @Override
    public int compare(IsNumericLiteral other) {
        DBSPI8Literal oi = other.to(DBSPI8Literal.class);
        Utilities.enforce(this.value != null);
        Utilities.enforce(oi.value != null);
        return this.value.compareTo(oi.value);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPI8Literal(this.getNode(), this.type, this.value);
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
        if (this.value == Byte.MIN_VALUE)
            throw new ArithmeticException("byte negation overflow");
        return new DBSPI8Literal(this.getNode(), this.type, (byte)-this.value);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPI8Literal(this.checkIfNull(this.value, mayBeNull), mayBeNull);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPI8Literal that = (DBSPI8Literal) o;
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
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

    @Override @Nullable
    public BigInteger getValue() {
        if (this.value == null)
            return null;
        return BigInteger.valueOf(this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPI8Literal fromJson(JsonNode node, JsonDecoder decoder) {
        Byte value = null;
        if (node.has("value"))
            value = (byte)Utilities.getIntProperty(node, "value");
        DBSPType type = getJsonType(node, decoder);
        return new DBSPI8Literal(CalciteObject.EMPTY, type, value);
    }
}
