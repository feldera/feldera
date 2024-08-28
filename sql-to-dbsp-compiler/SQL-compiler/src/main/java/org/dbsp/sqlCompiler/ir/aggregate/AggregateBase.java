package org.dbsp.sqlCompiler.ir.aggregate;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

/** Base class for aggregates */
public abstract class AggregateBase extends DBSPExpression implements IDBSPInnerNode {
    protected AggregateBase(CalciteObject node, DBSPType type) {
        super(node, type);
    }

    public abstract void validate();

    /** Result produced for an empty set */
    public abstract DBSPExpression getEmptySetResult();

    public DBSPType getType() {
        return this.getEmptySetResult().getType();
    }

    public abstract boolean isLinear();
}
