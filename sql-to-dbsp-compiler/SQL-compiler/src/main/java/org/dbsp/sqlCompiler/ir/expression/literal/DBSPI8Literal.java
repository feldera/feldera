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

    public DBSPI8Literal(CalciteObject node, DBSPType type , @Nullable Byte value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPI8Literal(@Nullable Byte value, boolean nullable) {
        this(CalciteObject.EMPTY, new DBSPTypeInteger(CalciteObject.EMPTY, 8, true, nullable), value);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
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
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPI8Literal(this.checkIfNull(this.value, mayBeNull), mayBeNull);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
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
        assert this.value != null;
        return this.value > 0;
    }
}
