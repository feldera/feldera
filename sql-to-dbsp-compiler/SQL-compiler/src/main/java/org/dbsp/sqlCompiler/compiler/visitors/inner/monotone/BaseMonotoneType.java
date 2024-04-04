package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.type.DBSPType;

/** Base class for monotone type information */
public abstract class BaseMonotoneType implements IMaybeMonotoneType {
    final DBSPType type;

    protected BaseMonotoneType(DBSPType type) {
        this.type = type;
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    /** Copy the monotonicity of the current type */
    public abstract IMaybeMonotoneType copyMonotonicity(DBSPType type);
}
