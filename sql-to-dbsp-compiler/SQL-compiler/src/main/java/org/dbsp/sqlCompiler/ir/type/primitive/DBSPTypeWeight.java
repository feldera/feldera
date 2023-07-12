package org.dbsp.sqlCompiler.ir.type.primitive;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;

/**
 * This type stands for the weights of an element in a collection.
 * The compiler decides how this is implemented.
 */
public class DBSPTypeWeight extends DBSPTypeBaseType {
    public static final DBSPTypeWeight INSTANCE = new DBSPTypeWeight();

    protected DBSPTypeWeight() {
        super(CalciteObject.EMPTY, DBSPTypeCode.WEIGHT, false);
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
        return other.is(DBSPTypeWeight.class);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull)
            throw new UnsupportedOperationException();
        return this;
    }

    @Override
    public DBSPLiteral defaultValue() {
        throw new UnimplementedException();
    }
}
