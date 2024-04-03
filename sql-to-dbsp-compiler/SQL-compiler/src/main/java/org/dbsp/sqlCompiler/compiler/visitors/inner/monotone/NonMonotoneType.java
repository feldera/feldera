package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** A type which has no monotone fields */
public class NonMonotoneType extends BaseMonotoneType {
    public NonMonotoneType(DBSPType type) {
        super(type);
    }

    @Override
    @Nullable
    public DBSPType getProjectedType() {
        return null;
    }

    @Override
    public boolean mayBeMonotone() {
        return false;
    }

    @Override
    public IMaybeMonotoneType copyMonotonicity(DBSPType type) {
        return new NonMonotoneType(type);
    }

    @Override
    public DBSPExpression projectExpression(DBSPExpression source) {
        throw new InternalCompilerError("Projecting a non-monotone type");
    }

    @Override
    public String toString() {
        return "NotMonotone(" + this.type + ")";
    }
}
