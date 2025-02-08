package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;

import javax.annotation.Nullable;

/** A monotone version of {@link org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd} */
public class CustomOrdMonotoneType
        extends BaseMonotoneType {
    final IMaybeMonotoneType wrappedType;
    final DBSPType exposedType;

    public CustomOrdMonotoneType(IMaybeMonotoneType type, DBSPType exposedType) {
        super();
        this.wrappedType = type;
        this.exposedType = exposedType;
        assert this.wrappedType.is(PartiallyMonotoneTuple.class);
        assert exposedType.is(DBSPTypeWithCustomOrd.class);
    }

    public int size() {
        return this.getWrappedType().size();
    }

    public PartiallyMonotoneTuple getWrappedType() {
        return this.wrappedType.to(PartiallyMonotoneTuple.class);
    }

    @Override
    public DBSPType getType() {
        return this.exposedType;
    }

    @Nullable
    @Override
    public DBSPType getProjectedType() {
        return this.wrappedType.getProjectedType();
    }

    @Override
    public boolean mayBeMonotone() {
        return this.wrappedType.mayBeMonotone();
    }

    @Override
    public DBSPExpression projectExpression(DBSPExpression source) {
        return this.wrappedType.projectExpression(new DBSPUnwrapCustomOrdExpression(source));
    }

    @Override
    public IMaybeMonotoneType withMaybeNull(boolean maybeNull) {
        throw new UnsupportedException(this.exposedType.getNode());
    }

    @Override
    public IMaybeMonotoneType union(IMaybeMonotoneType other) {
        throw new UnsupportedException(this.exposedType.getNode());
    }

    @Override
    public IMaybeMonotoneType intersection(IMaybeMonotoneType other) {
        throw new UnsupportedException(this.exposedType.getNode());
    }

    @Override
    public String toString() {
        return this.wrappedType.toString();
    }
}
