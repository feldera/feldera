package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;

/** A type which has no monotone fields. */
public class NonMonotoneType extends ScalarMonotoneType {
    public NonMonotoneType(DBSPType type) {
        super(type);
    }

    static PartiallyMonotoneTuple nonMonotoneTuple(DBSPTypeTupleBase tuple) {
        return new PartiallyMonotoneTuple(
                Linq.map(Linq.list(tuple.tupFields), NonMonotoneType::nonMonotone), tuple.isRaw(), tuple.mayBeNull);
    }

    /** Create a non-monotone version of the specified type */
    public static IMaybeMonotoneType nonMonotone(DBSPType type) {
        if (type.is(DBSPTypeBaseType.class) || type.is(DBSPTypeVec.class) ||
                type.is(DBSPTypeMap.class) || type.is(DBSPTypeAny.class)) {
            return new NonMonotoneType(type);
        } else if (type.is(DBSPTypeTupleBase.class)) {
            return nonMonotoneTuple(type.to(DBSPTypeTupleBase.class));
        } else if (type.is(DBSPTypeRef.class)) {
            return new MonotoneRefType(nonMonotone(type.to(DBSPTypeRef.class).ref()));
        }
        throw new UnsupportedException(type.getNode());
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
    public IMaybeMonotoneType setMaybeNull(boolean maybeNull) {
        return new NonMonotoneType(this.type.setMayBeNull(maybeNull));
    }

    @Override
    public IMaybeMonotoneType union(IMaybeMonotoneType other) {
        return other;
    }

    @Override
    public IMaybeMonotoneType intersection(IMaybeMonotoneType other) {
        return this;
    }

    @Override
    public String toString() {
        return this.type.toString();
    }
}
