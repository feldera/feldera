package org.dbsp.sqlCompiler.ir.aggregate;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

public class MinMaxAggregate extends AggregateBase {
    // TODO
    public MinMaxAggregate(CalciteObject node, DBSPType type) {
        super(node, type);
    }

    @Override
    public void validate() {

    }

    @Override
    public DBSPExpression getEmptySetResult() {
        return null;
    }

    @Override
    public boolean isLinear() {
        return false;
    }

    @Override
    public boolean compatible(AggregateBase other) {
        return false;
    }

    @Override
    public DBSPExpression deepCopy() {
        return null;
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        return false;
    }

    @Override
    public void accept(InnerVisitor visitor) {

    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        return false;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return null;
    }
}
