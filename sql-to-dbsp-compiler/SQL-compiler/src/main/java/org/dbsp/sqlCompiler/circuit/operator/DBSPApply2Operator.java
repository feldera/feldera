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

/** Equivalent to the apply2 operator from DBSP
 * which applies an arbitrary function to its 2 inputs.
 * The inputs and outputs do not have to be Z-sets or indexed Z-sets. */
public final class DBSPApply2Operator extends DBSPOperator {
    public DBSPApply2Operator(CalciteObject node, DBSPClosureExpression function,
                              DBSPType outputType, DBSPOperator left, DBSPOperator right,
                              @Nullable String comment) {
        super(node, "apply", function, outputType, false, comment);
        this.addInput(left);
        this.addInput(right);
        assert function.parameters.length == 2: "Expected 2 parameters for function " + function;
        DBSPType param0Type = function.parameters[0].getType().deref();
        assert left.outputType.sameType(param0Type):
                "Parameter type " + param0Type + " does not match input type " + left.outputType;
        DBSPType param1Type = function.parameters[1].getType().deref();
        assert right.outputType.sameType(param1Type):
                "Parameter type " + param1Type + " does not match input type " + right.outputType;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPApply2Operator(
                this.getNode(), Objects.requireNonNull(expression).to(DBSPClosureExpression.class),
                outputType, this.inputs.get(0), this.inputs.get(1), this.comment);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2: "Expected 2 inputs " + newInputs;
        if (force || this.inputsDiffer(newInputs)) {
            return new DBSPApply2Operator(
                    this.getNode(), this.getFunction().to(DBSPClosureExpression.class),
                    this.getType(), newInputs.get(0), newInputs.get(1), this.comment);
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
