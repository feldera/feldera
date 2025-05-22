package org.dbsp.sqlCompiler.ir.aggregate;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

/** Base class for aggregates */
public abstract class IAggregate extends DBSPExpression implements IDBSPInnerNode {
    protected IAggregate(CalciteObject node, DBSPType type) {
        super(node, type);
    }

    public abstract void validate();

    /** Result produced for an empty set */
    public abstract DBSPExpression getEmptySetResult();

    public DBSPType getType() {
        return this.getEmptySetResult().getType();
    }

    public abstract boolean isLinear();

    /** True if these two aggregates are "compatible", i.e. they
     * can be implemented in a single operator.  For example, all linear
     * aggregates are compatible with each other.
     * @param other   Aggregate to check compatibility with.
     * @param appendOnlySources  True if the current aggregate may depend on some append-only sources. */
    public abstract boolean compatible(IAggregate other, boolean appendOnlySources);
}
