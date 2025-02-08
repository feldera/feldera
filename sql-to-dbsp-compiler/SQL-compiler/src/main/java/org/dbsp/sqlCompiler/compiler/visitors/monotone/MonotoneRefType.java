package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** A monotone type that is a reference to another type */
public class MonotoneRefType
        extends BaseMonotoneType
        implements IMaybeMonotoneType {
    final DBSPType type;
    final IMaybeMonotoneType base;

    public MonotoneRefType(IMaybeMonotoneType base) {
        this.base = base;
        this.type = base.getType().ref();
    }

    @Override
    public IMaybeMonotoneType withMaybeNull(boolean maybeNull) {
        throw new UnsupportedException(this.type.getNode());
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
    public IMaybeMonotoneType union(IMaybeMonotoneType other) {
        return new MonotoneRefType(this.base.union(other.to(MonotoneRefType.class).base));
    }

    @Override
    public IMaybeMonotoneType intersection(IMaybeMonotoneType other) {
        return new MonotoneRefType(this.base.intersection(other.to(MonotoneRefType.class).base));
    }

    @Override
    public String toString() {
        return "Ref(" + this.base + ")";
    }
}
