package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.ICastable;
import org.dbsp.util.IHasId;

import javax.annotation.Nullable;
import java.util.List;

/** Interface representing the monotonicity as type information. */
public interface IMaybeMonotoneType extends ICastable, IHasId {
    /** The type whose monotonicity is represented */
    DBSPType getType();
    /** A type that contains only the monotone fields of this type.
     * May return 'null' if the type has no monotone components. */
    @Nullable
    DBSPType getProjectedType();
    /** True if the type has some monotone fields */
    boolean mayBeMonotone();
    /** Copy the monotonicity of the current type */
    default IMaybeMonotoneType copyMonotonicity(DBSPType type) {
        return this.to(ScalarMonotoneType.class).copyMonotonicity(type);
    }
    DBSPExpression projectExpression(DBSPExpression source);

    IMaybeMonotoneType withMaybeNull(boolean maybeNull);

    /** Given two monotone types, create a type that is the "union",
     * i.e., a field is monotone if it is monotone in *either* type. */
    IMaybeMonotoneType union(IMaybeMonotoneType other);

    /** Given two monotone types, create a type that is the "intersection",
     * i.e., a field is monotone if it is monotone in *both* types. */
    IMaybeMonotoneType intersection(IMaybeMonotoneType other);

    static IMaybeMonotoneType intersection(List<IMaybeMonotoneType> types) {
        IMaybeMonotoneType result = types.get(0);
        for (int i = 1; i < types.size(); i++)
            result = result.intersection(types.get(i));
        return result;
    }
}
