package org.dbsp.sqlCompiler.ir.type.primitive;

import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.VOID;

public class DBSPTypeVoid extends DBSPTypeBaseType {
    public DBSPTypeVoid() {
        super(CalciteObject.EMPTY, VOID, false);
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
    public boolean sameType(DBSPType other) {
        return other.is(DBSPTypeVoid.class);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull)
            throw new UnsupportedException(this.getNode());
        return this;
    }

    @Override
    public DBSPLiteral defaultValue() {
        throw new UnsupportedException(this.getNode());
    }
}
