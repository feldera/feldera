package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.ICastable;
import org.dbsp.util.IHasId;

/** A ValueProjection represents a subset of the "fields" from a value.
 * If the value is a scalar it is represented entirely.  This is more
 * interesting for tuples and structs. */
public abstract class ValueProjection implements ICastable, IHasId {
    static long crtId;
    public final long id;

    public final DBSPType type;

    ValueProjection(DBSPType type) {
        this.id = crtId++;
        this.type = type;
    }

    @Override
    public long getId() {
        return this.id;
    }

    public abstract boolean isEmpty();

    public DBSPType getType() {
        return this.type;
    }

    @Override
    public String toString() {
        return this.id + ": " + this.type;
    }

    public abstract DBSPType getProjectionResultType();

    /** Create the value of an input parameter.  The projected expression
     * is a field of a parameter.  The type of the parameter is the projectedType. */
    public abstract MonotoneValue createInput(DBSPExpression projected);

    /** Project the specified expression returning only the fields
     * described by this projection */
    public abstract DBSPExpression project(DBSPExpression expression);
}
