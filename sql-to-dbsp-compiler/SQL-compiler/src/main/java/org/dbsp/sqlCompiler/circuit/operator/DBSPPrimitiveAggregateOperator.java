package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

/** This is a primitive operator that corresponds to the Rust AggregateIncremental node. */
@NonCoreIR
public final class DBSPPrimitiveAggregateOperator extends DBSPOperator {
    public DBSPPrimitiveAggregateOperator(
            CalciteObject node, @Nullable DBSPExpression function, DBSPType outputType,
            DBSPOperator delta, DBSPOperator integral) {
        super(node, "AggregateIncremental", function, outputType, false);
        this.addInput(delta);
        this.addInput(integral);
        assert delta.getOutputIndexedZSetType().sameType(integral.getOutputIndexedZSetType());
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPPrimitiveAggregateOperator(this.getNode(), expression,
                outputType, this.inputs.get(0), this.inputs.get(1));
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2: "Expected 2 inputs";
        if (force || this.inputsDiffer(newInputs))
            return new DBSPPrimitiveAggregateOperator(this.getNode(), this.function,
                    this.outputType, newInputs.get(0), newInputs.get(1));
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
