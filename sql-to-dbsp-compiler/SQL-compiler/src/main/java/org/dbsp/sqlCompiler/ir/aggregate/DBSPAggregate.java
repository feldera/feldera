package org.dbsp.sqlCompiler.ir.aggregate;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import java.util.List;

/** Description of an aggregate.
 * In general an aggregate performs multiple simple aggregates simultaneously. */
public final class DBSPAggregate extends DBSPNode
        implements IDBSPInnerNode, IDBSPDeclaration // Declares the row variable
{
    public final DBSPVariablePath rowVar;
    /** Component aggregates, must all be linear or non-linear */
    public final List<AggregateBase> aggregates;
    // Cache here the result of aggregation on the empty set
    final DBSPExpression emptySetResult;
    final boolean isLinear;

    public DBSPAggregate(CalciteObject node, DBSPVariablePath rowVar,
                         List<AggregateBase> aggregates) {
        super(node);
        assert !aggregates.isEmpty();
        this.rowVar = rowVar;
        this.aggregates = aggregates;
        this.isLinear = Linq.all(aggregates, AggregateBase::isLinear);
        this.emptySetResult = new DBSPTupleExpression(node, Linq.map(aggregates, AggregateBase::getEmptySetResult));
        for (AggregateBase b: this.aggregates)
            assert b.isLinear() == this.isLinear;
    }

    public boolean isLinear() {
        return this.isLinear;
    }

    public DBSPTypeTuple getEmptySetResultType() {
        return this.getEmptySetResult().getType().to(DBSPTypeTuple.class);
    }

    public DBSPExpression getEmptySetResult() {
        return this.emptySetResult;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("aggregates");
        int index = 0;
        for (AggregateBase impl : this.aggregates) {
            visitor.propertyIndex(index++);
            impl.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    public DBSPExpression compact(DBSPCompiler compiler) {
        if (this.isLinear())
            return this.asLinear(compiler);
        else
            return this.asFold(compiler, true);
    }

    public DBSPExpression asFold(DBSPCompiler compiler, boolean compact) {
        assert !this.isLinear();
        NonLinearAggregate combined = NonLinearAggregate.combine(
                this.getNode(), compiler, this.rowVar,
                Linq.map(this.aggregates, a -> a.to(NonLinearAggregate.class)));
        return combined.asFold(compact);
    }

    public LinearAggregate asLinear(DBSPCompiler compiler) {
        assert this.isLinear();
        return LinearAggregate.combine(
                this.getNode(), compiler, this.rowVar,
                Linq.map(this.aggregates, a -> a.to(LinearAggregate.class)));
    }

    public DBSPExpression asFold(DBSPCompiler compiler) {
        return this.asFold(compiler, false);
    }

    public int size() {
        return this.aggregates.size();
    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    @Override
    public String getName() {
        return this.rowVar.variable;
    }

    @Override
    public DBSPType getType() {
        return this.getEmptySetResultType();
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPAggregate o = other.as(DBSPAggregate.class);
        if (o == null)
            return false;
        return this.rowVar == o.rowVar &&
                Linq.same(this.aggregates, o.aggregates);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("Aggregate ")
                .append(this.aggregates.size())
                .append(":").increase();
        for (AggregateBase impl : this.aggregates) {
            builder.append(impl);
        }
        return builder.decrease();
    }

    public boolean equivalent(DBSPAggregate other) {
        if (this.size() != other.size())
            return false;
        EquivalenceContext context = new EquivalenceContext();
        context.leftDeclaration.newContext();
        context.rightDeclaration.newContext();
        context.leftDeclaration.substitute(this.rowVar.variable, this);
        context.rightDeclaration.substitute(other.rowVar.variable, other);
        context.leftToRight.substitute(this, other);
        return context.equivalent(this.aggregates, other.aggregates);
    }
}
