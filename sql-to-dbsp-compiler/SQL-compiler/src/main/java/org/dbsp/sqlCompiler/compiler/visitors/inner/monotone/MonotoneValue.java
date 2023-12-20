package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.ICastable;
import org.dbsp.util.IHasId;

public abstract class MonotoneValue implements IHasId, ICastable {
    static long crtId = 0;
    public final long id;

    public final DBSPType originalType;

    protected MonotoneValue(DBSPType originalType) {
        this.originalType = originalType;
        this.id = crtId++;
    }

    @Override
    public long getId() {
        return this.id;
    }

    public DBSPType getOriginalType() {
        return this.originalType;
    }

    public DBSPType getType() {
        return this.getExpression().getType();
    }

    public MonotoneValue ref() {
        return new MonotoneRef(this);
    }

    /** Expression which, when read, gives the value of this monotone value */
    public abstract DBSPExpression getExpression();

    @Override
    public String toString() {
        return this.id + ": " + this.getClass().getSimpleName() + ": " + this.getExpression() +
                ": " + this.getType();
    }

    public abstract ValueProjection getProjection();

    public abstract boolean isEmpty();
}
