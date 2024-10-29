package org.dbsp.sqlCompiler.ir.type.primitive;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.IIndentStream;

/** Represents a SQL VARIANT type: dynamically-typed values. */
public class DBSPTypeVariant extends DBSPTypeBaseType {
    public DBSPTypeVariant(CalciteObject node, boolean mayBeNull) {
        super(node, DBSPTypeCode.VARIANT, mayBeNull);
    }

    public DBSPTypeVariant(boolean mayBeNull) {
        this(CalciteObject.EMPTY, mayBeNull);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeVariant.class);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeVariant(this.getNode(), mayBeNull);
    }

    @Override
    public DBSPExpression defaultValue() {
        return DBSPVariantLiteral.sqlNull(this.mayBeNull);
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
        return builder.append("VARIANT");
    }

    @Override
    public boolean hasCopy() {
        return false;
    }
}
