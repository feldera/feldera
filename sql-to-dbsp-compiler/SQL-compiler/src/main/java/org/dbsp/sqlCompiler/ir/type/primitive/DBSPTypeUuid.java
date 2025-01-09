package org.dbsp.sqlCompiler.ir.type.primitive;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUuidLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;

import java.util.Objects;
import java.util.UUID;

/** A 128-bit UUID */
public class DBSPTypeUuid extends DBSPTypeBaseType {
    public DBSPTypeUuid(CalciteObject node, boolean mayBeNull) {
        super(node, DBSPTypeCode.UUID, mayBeNull);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeUuid.class);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeUuid(this.getNode(), mayBeNull);
    }

    @Override
    public DBSPExpression defaultValue() {
        return new DBSPUuidLiteral(this.getNode(), this, new UUID(0, 0));
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
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 2);
    }

    @Override
    public boolean hasCopy() {
        return false;
    }
}
