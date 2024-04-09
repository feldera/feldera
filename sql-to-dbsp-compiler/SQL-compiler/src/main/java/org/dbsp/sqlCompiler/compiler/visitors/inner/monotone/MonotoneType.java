package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** A type which is completely monotone (all fields are monotone) */
public class MonotoneType extends BaseMonotoneType {
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
    public String toString() {
        return "Monotone(" + this.type + ")";
    }
}
