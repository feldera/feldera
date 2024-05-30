package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

@NonCoreIR
public final class DBSPPartitionedTreeAggregateOperator extends DBSPUnaryOperator {
    @Nullable
    public final DBSPAggregate aggregate;

    public DBSPPartitionedTreeAggregateOperator(
            CalciteObject node, @Nullable DBSPExpression function, @Nullable DBSPAggregate aggregate,
            DBSPType outputType, DBSPOperator input) {
        super(node, "partitioned_tree_aggregate", function, outputType, false, input);
        this.aggregate = aggregate;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPPartitionedTreeAggregateOperator otherOperator = other.as(DBSPPartitionedTreeAggregateOperator.class);
        if (otherOperator == null)
            return false;
        return EquivalenceContext.equiv(this.aggregate, otherOperator.aggregate);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPPartitionedTreeAggregateOperator(this.getNode(), expression, this.aggregate,
                outputType, this.inputs.get(0));
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 1: "Expected 1 input";
        if (force || this.inputsDiffer(newInputs))
            return new DBSPPartitionedTreeAggregateOperator(
                    this.getNode(), this.function, this.aggregate, this.outputType, newInputs.get(0));
        return this;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }
}
