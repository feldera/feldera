package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.ICastable;

import javax.annotation.Nullable;

/** Interfae representing the monotonicity as type information. */
public interface IMaybeMonotoneType extends ICastable {
    /** The type whose monotonicity is represented */
    DBSPType getType();
    /** A type that contains only the monotone fields of this type.
     * Returns 'null' if the type has no monotone components. */
    @Nullable
    DBSPType getProjectedType();
    /** True if the type has some monotone fields */
    boolean mayBeMonotone();
    /** Copy the monotonicity of the current type */
    default IMaybeMonotoneType copyMonotonicity(DBSPType type) {
        return this.to(BaseMonotoneType.class).copyMonotonicity(type);
    }
    DBSPExpression projectExpression(DBSPExpression source);
}
