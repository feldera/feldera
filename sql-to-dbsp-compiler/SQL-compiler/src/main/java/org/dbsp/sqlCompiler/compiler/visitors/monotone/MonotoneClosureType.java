package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;

/** The monotone type of a closure expression */
public class MonotoneClosureType
        extends BaseMonotoneType
        implements IMaybeMonotoneType {
    private final IMaybeMonotoneType bodyType;
    /** Parameters of the original closure whose monotonicity is represented */
    private final DBSPParameter[] params;
    /** Parameters of the projected closure, containing only fields that are known
     * to be monotone in the input */
    private final DBSPParameter[] projectedParameters;

    public MonotoneClosureType(IMaybeMonotoneType bodyType,
                               DBSPParameter[] params,
                               DBSPParameter[] projectedParameters) {
        this.bodyType = bodyType;
        this.params = params;
        this.projectedParameters = projectedParameters;
    }

    @Override
    public DBSPType getType() {
        return new DBSPTypeFunction(this.bodyType.getType(), parameterTypes(this.params));
    }

    public IMaybeMonotoneType getBodyType() {
        return this.bodyType;
    }

    @Nullable
    @Override
    public DBSPType getProjectedType() {
        DBSPType pBodyType = this.bodyType.getProjectedType();
        if (pBodyType == null)
            return null;
        return new DBSPTypeFunction(pBodyType, parameterTypes(this.projectedParameters));
    }

    static DBSPType[] parameterTypes(DBSPParameter[] params) {
        return Linq.map(params, p -> p.type, DBSPType.class);
    }

    @Override
    public DBSPExpression projectExpression(DBSPExpression source) {
        throw new InternalCompilerError("Projecting a closure");
    }

    @Override
    public IMaybeMonotoneType withMaybeNull(boolean maybeNull) {
        throw new UnsupportedException(this.getType().getNode());
    }

    @Override
    public IMaybeMonotoneType union(IMaybeMonotoneType other) {
        throw new InternalCompilerError("'union' called on a MonotoneClosureType");
    }

    @Override
    public IMaybeMonotoneType intersection(IMaybeMonotoneType other) {
        throw new InternalCompilerError("'intersection' called on a MonotoneClosureType");
    }

    @Override
    public boolean mayBeMonotone() {
        return this.bodyType.mayBeMonotone();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("MonotoneClosure(|");
        for (DBSPParameter param: this.projectedParameters)
            builder.append(param.type).append(",");
        builder.append("| -> ")
                .append(this.bodyType)
                .append(")");
        return builder.toString();
    }
}
