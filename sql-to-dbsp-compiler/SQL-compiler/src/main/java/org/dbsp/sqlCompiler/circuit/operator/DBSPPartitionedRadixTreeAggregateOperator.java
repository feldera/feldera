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
public final class DBSPPartitionedRadixTreeAggregateOperator extends DBSPOperator {
    @Nullable
    public final DBSPAggregate aggregate;

    public DBSPPartitionedRadixTreeAggregateOperator(
            CalciteObject node, @Nullable DBSPExpression function, @Nullable DBSPAggregate aggregate,
            DBSPOperator input, DBSPOperator inputIntegral, DBSPOperator outputIntegral) {
        super(node, "PartitionedRadixTreeAggregate", function, outputIntegral.getType(), false);
        this.addInput(input);
        this.addInput(inputIntegral);
        this.addInput(outputIntegral);
        this.aggregate = aggregate;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPPartitionedRadixTreeAggregateOperator(this.getNode(), expression, this.aggregate,
                this.inputs.get(0), this.inputs.get(1), this.inputs.get(2));
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 3: "Expected 3 inputs";
        if (force || this.inputsDiffer(newInputs))
            return new DBSPPartitionedRadixTreeAggregateOperator(
                    this.getNode(), this.getFunction(), this.aggregate,
                    newInputs.get(0), newInputs.get(1), newInputs.get(2));
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPPartitionedRadixTreeAggregateOperator otherOperator =
                other.as(DBSPPartitionedRadixTreeAggregateOperator.class);
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
