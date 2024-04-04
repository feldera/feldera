package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;

import javax.annotation.Nullable;

/** The monotone type of a closure expression */
public class MonotoneClosureType implements IMaybeMonotoneType {
    private final IMaybeMonotoneType bodyType;
    /** Parameter of the original closure whose monotonicity is represented */
    private final DBSPParameter param;
    /** Parameter of the projected closure, containing only fields that are known
     * to be monotone in the input */
    private final DBSPParameter projectedParameter;

    public MonotoneClosureType(IMaybeMonotoneType bodyType,
                               DBSPParameter param, DBSPParameter projectedParameter) {
        this.bodyType = bodyType;
        this.param = param;
        this.projectedParameter = projectedParameter;
    }

    @Override
    public DBSPType getType() {
        return new DBSPTypeFunction(this.bodyType.getType(), this.param.type);
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
        return new DBSPTypeFunction(
                pBodyType, this.projectedParameter.type);
    }

    @Override
    public DBSPExpression projectExpression(DBSPExpression source) {
        throw new InternalCompilerError("Projecting a closure");
    }

    @Override
    public boolean mayBeMonotone() {
        return this.bodyType.mayBeMonotone();
    }

    @Override
    public String toString() {
        return "MonotoneClosure(|" + this.projectedParameter.type + "| -> " + this.bodyType + ")";
    }
}
