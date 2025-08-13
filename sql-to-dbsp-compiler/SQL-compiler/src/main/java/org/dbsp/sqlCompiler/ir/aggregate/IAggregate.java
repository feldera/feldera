package org.dbsp.sqlCompiler.ir.aggregate;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.util.List;

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

    /** Combine all the aggregates in the list into a single aggregate.
     * Note: 'this' is not used in the result; it is expected to be the
     * first element of 'list'.
     * @param node       Calcite node.
     * @param compiler   Compiler used to simplify formulas.
     * @param rowVar     Row variable common to all aggregates in list.
     * @param list       List of aggregates to combine.
     * @return           A new aggregate that does the work of all the aggregates in the list in a single operation.
     */
    public abstract IAggregate combine(
            CalciteObject node, DBSPCompiler compiler, DBSPVariablePath rowVar, List<IAggregate> list);

    public abstract boolean isLinear();

    /** Return references to all parameters inside the aggregate that refer to the row */
    public abstract List<DBSPParameter> getRowVariableReferences();

    /** True if these two aggregates are "compatible", i.e. they
     * can be implemented in a single operator.  For example, all linear
     * aggregates are compatible with each other.
     * @param other   Aggregate to check compatibility with. */
    public abstract boolean compatible(IAggregate other);
}
