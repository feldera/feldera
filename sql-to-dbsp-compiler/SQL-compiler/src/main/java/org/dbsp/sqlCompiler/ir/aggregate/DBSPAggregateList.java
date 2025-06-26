package org.dbsp.sqlCompiler.ir.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.List;

/** Description of a list of aggregations. */
public final class DBSPAggregateList extends DBSPNode
        implements IDBSPInnerNode, IDBSPDeclaration // Declares the row variable
{
    public final DBSPVariablePath rowVar;
    /** Component aggregates, must all be linear or non-linear */
    public final List<IAggregate> aggregates;
    // Cache here the result of aggregation on the empty set
    final DBSPExpression emptySetResult;
    final boolean isLinear;

    public DBSPAggregateList(CalciteObject node, DBSPVariablePath rowVar,
                             List<IAggregate> aggregates) {
        super(node);
        Utilities.enforce(!aggregates.isEmpty());
        this.rowVar = rowVar;
        this.aggregates = aggregates;
        this.isLinear = Linq.all(aggregates, IAggregate::isLinear);
        this.emptySetResult = new DBSPTupleExpression(node, Linq.map(aggregates, IAggregate::getEmptySetResult));
        for (IAggregate b: this.aggregates) {
            Utilities.enforce(b.isLinear() == this.isLinear);
            List<DBSPParameter> params = b.getRowVariableReferences();
            for (DBSPParameter p: params) {
                Utilities.enforce(this.rowVar.getType().sameType(p.getType()));
                Utilities.enforce(this.rowVar.variable.equals(p.name));
            }
        }
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
        visitor.property("rowVar");
        this.rowVar.accept(visitor);
        visitor.property("aggregates");
        int index = 0;
        for (IAggregate impl : this.aggregates) {
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
            return this.asFold(compiler);
    }

    public DBSPFold asFold(DBSPCompiler compiler) {
        Utilities.enforce(!this.isLinear());
        NonLinearAggregate combined = NonLinearAggregate.combine(
                this.getNode(), compiler, this.rowVar,
                Linq.map(this.aggregates, a -> a.to(NonLinearAggregate.class)));
        return combined.asFold();
    }

    public LinearAggregate asLinear(DBSPCompiler compiler) {
        Utilities.enforce(this.isLinear());
        return LinearAggregate.combine(
                this.getNode(), compiler, this.rowVar,
                Linq.map(this.aggregates, a -> a.to(LinearAggregate.class)));
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
        return new DBSPTypeFunction(this.getEmptySetResultType(), this.rowVar.getType());
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPAggregateList o = other.as(DBSPAggregateList.class);
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
        for (IAggregate impl : this.aggregates) {
            builder.append(impl);
        }
        return builder.decrease();
    }

    public boolean equivalent(DBSPAggregateList other) {
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

    @SuppressWarnings("unused")
    public static DBSPAggregateList fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPVariablePath rowVar = fromJsonInner(node, "rowVar", decoder, DBSPVariablePath.class);
        List<IAggregate> aggregates = fromJsonInnerList(node, "aggregates", decoder, IAggregate.class);
        return new DBSPAggregateList(CalciteObject.EMPTY, rowVar, aggregates);
    }
}
