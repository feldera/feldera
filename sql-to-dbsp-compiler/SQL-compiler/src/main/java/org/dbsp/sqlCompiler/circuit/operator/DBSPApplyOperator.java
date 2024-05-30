package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Equivalent to the apply operator from DBSP
 * which applies an arbitrary function to the input(s).
 * The inputs and outputs do not have to be Z-sets or indexed Z-sets. */
public final class DBSPApplyOperator extends DBSPUnaryOperator {
    public DBSPApplyOperator(CalciteObject node, DBSPClosureExpression function,
                             DBSPType outputType, DBSPOperator input, @Nullable String comment) {
        super(node, "apply", function, outputType, false, input, comment);
        assert function.parameters.length == 1: "Expected 1 parameter for function " + function;
        DBSPType paramType = function.parameters[0].getType().deref();
        assert input.outputType.sameType(paramType):
                "Parameter type " + paramType + " does not match input type " + input.outputType;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPApplyOperator(
                this.getNode(), Objects.requireNonNull(expression).to(DBSPClosureExpression.class),
                outputType, this.input(), this.comment);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 1: "Expected 1 input " + newInputs;
        if (force || this.inputsDiffer(newInputs)) {
            return new DBSPApplyOperator(
                    this.getNode(), this.getFunction().to(DBSPClosureExpression.class),
                    this.getType(), newInputs.get(0), this.comment);
        }
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
