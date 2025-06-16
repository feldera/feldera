package org.dbsp.sqlCompiler.ir.expression.literal;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

public final class DBSPU8Literal extends DBSPIntLiteral implements IsNumericLiteral {
    @Nullable
    public final Integer value;

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPU8Literal that = (DBSPU8Literal) o;
        return Objects.equals(value, that.value);
    }

    public DBSPU8Literal() {
        this(null, true);
    }

    public DBSPU8Literal(CalciteObject node, DBSPType type, @Nullable Integer value) {
        super(node, type, value == null);
        if (value != null && value < 0)
            throw new CompilationError("Negative value for u8 literal " + value);
        if (value != null && value > 255)
            throw new CompilationError("Value too large for u8 literal " + value);
        this.value = value;
    }

    @Override
    public boolean gt0() {
        Utilities.enforce(this.value != null);
        return this.value > 0;
    }

    @Override
    public IsNumericLiteral negate() {
        throw new UnsupportedException("Negation of unsigned values", this.getNode());
    }

    public DBSPU8Literal(Integer value) {
        this(value, false);
    }

    public DBSPU8Literal(@Nullable Integer value, boolean nullable) {
        this(CalciteObject.EMPTY, DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.UINT8, nullable), value);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
        if (value != null && value < 0)
            throw new InternalCompilerError("Negative value for unsigned literal", this);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPU8Literal(this.getNode(), this.type, this.value);
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
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPU8Literal(this.checkIfNull(this.value, mayBeNull), mayBeNull);
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
        throw new InternalCompilerError("unreachable");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Nullable
    @Override
    public BigInteger getValue() {
        if (this.value == null)
            return null;
        return BigInteger.valueOf(this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPU8Literal fromJson(JsonNode node, JsonDecoder decoder) {
        Integer value = null;
        if (node.has("value"))
            value = Utilities.getIntProperty(node, "value");
        DBSPType type = getJsonType(node, decoder);
        return new DBSPU8Literal(CalciteObject.EMPTY, type, value);
    }
}
