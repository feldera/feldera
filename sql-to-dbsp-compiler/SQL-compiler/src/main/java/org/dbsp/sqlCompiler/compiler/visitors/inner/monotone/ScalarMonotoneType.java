package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;

public abstract class ScalarMonotoneType
        extends BaseMonotoneType
        implements IMaybeMonotoneType {
    final DBSPType type;

    // "Scalar" types include Vec and Map.

    protected ScalarMonotoneType(DBSPType type) {
        super();
        assert !type.is(DBSPTypeTupleBase.class) :
            "Type is expected to be scalar " + type;
        this.type = type;
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    /** Copy the monotonicity of the current type */
    public abstract IMaybeMonotoneType copyMonotonicity(DBSPType type);
}
