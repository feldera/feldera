package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** A type which is completely monotone (all fields are monotone) */
public class MonotoneType extends ScalarMonotoneType {
    public MonotoneType(DBSPType type) {
        super(type);
    }

    @Override
    @Nullable public DBSPType getProjectedType() {
        return this.type;
    }

    @Override
    public boolean mayBeMonotone() {
        return true;
    }

    @Override
    public IMaybeMonotoneType copyMonotonicity(DBSPType type) {
        return new MonotoneType(type);
    }

    @Override
    public DBSPExpression projectExpression(DBSPExpression source) {
        return source;
    }

    @Override
    public IMaybeMonotoneType withMaybeNull(boolean maybeNull) {
        return new MonotoneType(this.type.withMayBeNull(maybeNull));
    }

    @Override
    public IMaybeMonotoneType union(IMaybeMonotoneType other) {
        return this;
    }

    @Override
    public IMaybeMonotoneType intersection(IMaybeMonotoneType other) {
        return other;
    }

    @Override
    public String toString() {
        return "M(" + this.type + ")";
    }
}
