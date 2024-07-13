package org.dbsp.sqlCompiler.ir.type.primitive;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.BYTES;

/**
 * Represents a byte array.
 */
public class DBSPTypeBinary extends DBSPTypeBaseType {
    public static final int UNLIMITED_PRECISION = -1;

    public final int precision;

    public DBSPTypeBinary(CalciteObject node, int precision, boolean mayBeNull) {
        super(node, BYTES, mayBeNull);
        this.precision = precision;
    }

    public DBSPTypeBinary(CalciteObject node, boolean mayBeNull) {
        super(node, BYTES, mayBeNull);
        this.precision = UNLIMITED_PRECISION;
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeBinary(this.getNode(), this.precision, mayBeNull);
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return new DBSPBinaryLiteral(new byte[] {}, this.mayBeNull);
    }

    @Override
    public boolean sameType(DBSPType type) {
        DBSPTypeBinary other = type.as(DBSPTypeBinary.class);
        if (other == null)
            return false;
        if (!super.sameNullability(type))
            return false;
        return this.precision == other.precision;
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
}

