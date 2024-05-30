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
public final class DBSPPartitionedRollingAggregateOperator extends DBSPOperator {
    @Nullable
    public final DBSPAggregate aggregate;

    public DBSPPartitionedRollingAggregateOperator(
            CalciteObject node, @Nullable DBSPExpression function, @Nullable DBSPAggregate aggregate,
            DBSPOperator input, DBSPOperator inputIntegral,
            DBSPOperator tree, DBSPOperator outputIntegral) {
        super(node, "PartitionedRollingAggregate", null, outputIntegral.getType(), false);
        this.addInput(input);
        this.addInput(inputIntegral);
        this.addInput(tree);
        this.addInput(outputIntegral);
        this.aggregate = aggregate;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPPartitionedRollingAggregateOperator(this.getNode(), expression, this.aggregate,
                this.inputs.get(0), this.inputs.get(1), this.inputs.get(2), this.inputs.get(3));
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 4: "Expected 4 inputs";
        if (force || this.inputsDiffer(newInputs))
            return new DBSPPartitionedRollingAggregateOperator(
                    this.getNode(), this.function, this.aggregate,
                    newInputs.get(0), newInputs.get(1), newInputs.get(2), newInputs.get(3));
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPPartitionedRollingAggregateOperator otherOperator =
                other.as(DBSPPartitionedRollingAggregateOperator.class);
        if (otherOperator == null)
            return false;
        return EquivalenceContext.equiv(this.aggregate, otherOperator.aggregate);
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
