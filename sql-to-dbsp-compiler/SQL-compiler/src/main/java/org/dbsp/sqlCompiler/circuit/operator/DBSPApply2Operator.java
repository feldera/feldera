package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
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
 * The inputs and outputs cannot be Z-sets or indexed Z-sets.
 * The comments from {@link DBSPApplyOperator} apply to this operator as well. */
public final class DBSPApply2Operator extends DBSPBinaryOperator {
    public DBSPApply2Operator(CalciteRelNode node, DBSPClosureExpression function,
                              OutputPort left, OutputPort right) {
        super(node, "apply2", function, function.getResultType(), false, left, right);
        assert function.parameters.length == 2: "Expected 2 parameters for function " + function;
        DBSPType param0Type = function.parameters[0].getType().deref();
        assert left.outputType().sameType(param0Type):
                "Parameter type " + param0Type + " does not match input type " + left.outputType();
        DBSPType param1Type = function.parameters[1].getType().deref();
        assert right.outputType().sameType(param1Type):
                "Parameter type " + param1Type + " does not match input type " + right.outputType();
        DBSPApplyOperator.noZsets(left.outputType());
        DBSPApplyOperator.noZsets(right.outputType());
        DBSPApplyOperator.noZsets(this.outputType());
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPApply2Operator(
                this.getRelNode(), Objects.requireNonNull(expression).to(DBSPClosureExpression.class),
                this.left(), this.right())
                .copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        assert newInputs.size() == 2: "Expected 2 inputs " + newInputs;
        if (force || this.inputsDiffer(newInputs)) {
            return new DBSPApply2Operator(
                    this.getRelNode(), this.getClosureFunction(),
                    newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
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

    @SuppressWarnings("unused")
    public static DBSPApply2Operator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPApply2Operator(
                CalciteEmptyRel.INSTANCE, info.getClosureFunction(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPApply2Operator.class);
    }
}
