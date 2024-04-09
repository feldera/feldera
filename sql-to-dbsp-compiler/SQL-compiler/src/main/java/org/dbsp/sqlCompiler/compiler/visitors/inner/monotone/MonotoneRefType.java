package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** A monotone type that is a reference to another type */
public class MonotoneRefType implements IMaybeMonotoneType {
    final DBSPType type;
    final IMaybeMonotoneType base;

    public MonotoneRefType(IMaybeMonotoneType base) {
        this.base = base;
        this.type = base.getType().ref();
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    @Nullable
    @Override
    public DBSPType getProjectedType() {
        DBSPType type = this.base.getProjectedType();
        if (type == null)
            return null;
        return type.ref();
    }

    @Override
    public boolean mayBeMonotone() {
        return this.base.mayBeMonotone();
    }

    @Override
    public DBSPExpression projectExpression(DBSPExpression source) {
        return this.base.projectExpression(source.deref()).borrow();
    }

    @Override
    public String toString() {
        return "Ref(" + this.base + ")";
    }
}
