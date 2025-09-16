package org.dbsp.sqlCompiler.ir.type.primitive;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.BYTES;

/** Represents a byte array. */
public class DBSPTypeBinary extends DBSPTypeBaseType implements IHasPrecision {
    /** If true the width is fixed, i.e., this is a CHAR type.
     * Otherwise, this is a VARCHAR. */
    public final boolean fixed;
    /** Number of bytes; this may be UNLIMITED_PRECISION */
    public final int precision;

    public DBSPTypeBinary(CalciteObject node, int precision, boolean fixed, boolean mayBeNull) {
        super(node, BYTES, mayBeNull);
        this.fixed = fixed;
        this.precision = precision;
    }

    public DBSPTypeBinary(CalciteObject node, boolean fixed, boolean mayBeNull) {
        this(node, UNLIMITED_PRECISION, fixed, mayBeNull);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeBinary(this.getNode(), this.precision, this.fixed, mayBeNull);
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return new DBSPBinaryLiteral(this.getNode(), this, new byte[] {});
    }

    @Override
    public boolean sameType(DBSPType type) {
        DBSPTypeBinary other = type.as(DBSPTypeBinary.class);
        if (other == null)
            return false;
        if (!super.sameNullability(type))
            return false;
        return this.precision == other.precision && this.fixed == other.fixed;
    }

    @Override
    public boolean hasCopy() {
        return false;
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
    public IIndentStream toString(IIndentStream builder) {
        if (this.precision == UNLIMITED_PRECISION)
            return super.toString(builder);
        return builder.append(this.shortName())
                .append("(")
                .append(this.precision)
                .append(")")
                .append(this.mayBeNull ? "?" : "");
    }

    @Nullable
    public String asSqlString() {
        String result = this.fixed ? "BINARY" : "VARBINARY";
        if (this.precision != UNLIMITED_PRECISION) {
            result += "(" + this.precision + ")";
        }
        return result;
    }

    @Override
    public int getPrecision() {
        return this.precision;
    }

    @SuppressWarnings("unused")
    public static DBSPTypeBinary fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        int precision = Utilities.getIntProperty(node, "precision");
        boolean fixed = Utilities.getBooleanProperty(node, "fixed");
        return new DBSPTypeBinary(CalciteObject.EMPTY, precision, fixed, mayBeNull);
    }
}
