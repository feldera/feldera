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
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

public final class DBSPU128Literal extends DBSPIntLiteral implements IsNumericLiteral {
    @Nullable
    public final BigInteger value;

    public DBSPU128Literal(CalciteObject node, DBSPType type, @Nullable BigInteger value) {
        super(node, type, value == null);
        if (value != null && value.compareTo(BigInteger.ZERO) < 0)
            throw new CompilationError("Negative value for u128 literal " + value);
        this.value = value;
    }

    public DBSPU128Literal(CalciteObject node, @Nullable BigInteger value, boolean nullable) {
        this(node, new DBSPTypeInteger(CalciteObject.EMPTY, 128, false, nullable), value);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
    }

    public DBSPU128Literal(@Nullable BigInteger value, boolean nullable) {
        this(CalciteObject.EMPTY, value, nullable);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPU128Literal that = (DBSPU128Literal) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public boolean gt0() {
        assert this.value != null;
        return this.value.compareTo(BigInteger.ZERO) > 0;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPU128Literal(this.getNode(), this.type, this.value);
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
        throw new UnsupportedException("Negation of unsigned values", this.getNode());
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPU128Literal(this.checkIfNull(this.value, mayBeNull), mayBeNull);
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

    @Override @Nullable
    public BigInteger getValue() {
        return this.value;
    }

    @SuppressWarnings("unused")
    public static DBSPU128Literal fromJson(JsonNode node, JsonDecoder decoder) {
        BigInteger value = null;
        if (node.has("value"))
            value = new BigInteger(Utilities.getStringProperty(node, "value"));
        DBSPType type = getJsonType(node, decoder);
        return new DBSPU128Literal(CalciteObject.EMPTY, type, value);
    }
}
