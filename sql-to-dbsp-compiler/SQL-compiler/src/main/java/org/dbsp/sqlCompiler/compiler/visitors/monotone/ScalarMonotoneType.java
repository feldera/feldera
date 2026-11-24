package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.Utilities;

public abstract class ScalarMonotoneType
        extends BaseMonotoneType {
    final DBSPType type;

    // "Scalar" types include Vec and Map.

    protected ScalarMonotoneType(DBSPType type) {
        super();
        if (type.is(DBSPTypeTupleBase.class))
            throw expectedScalar(type);
        this.type = type;
    }

    public static InternalCompilerError expectedScalar(DBSPType type) {
        return new InternalCompilerError("Type should have been scalar " + type
                + System.lineSeparator() + Utilities.getCurrentStackTrace());
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    /** Copy the monotonicity of the current type */
    public abstract IMaybeMonotoneType copyMonotonicity(DBSPType type);
}
