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
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Equivalent to the apply_n operator from DBSP
 * which applies an arbitrary function to its N inputs.
 * The inputs and outputs cannot be Z-sets or indexed Z-sets.
 * The comments from {@link DBSPApplyOperator} apply to this operator as well. */
public final class DBSPApplyNOperator extends DBSPSimpleOperator implements ILinear {
    public DBSPApplyNOperator(CalciteRelNode node, DBSPClosureExpression function,
                              List<OutputPort> inputs) {
        super(node, "apply_n", function, function.getResultType(), false);
        for (var input: inputs)
            this.addInput(input);
        Utilities.enforce(function.parameters.length == inputs.size(),
                () -> "Expected " + inputs.size() + " parameters for function " + function);
        for (int i = 0; i < inputs.size(); i++) {
            DBSPType paramIType = function.parameters[i].getType().deref();
            var inputType = inputs.get(i).outputType();
            int finalI = i;
            Utilities.enforce(inputType.sameType(paramIType),
                    () -> "Parameter " + finalI + " type " + paramIType + " does not match input type " + inputType);
            DBSPApplyOperator.noZsets(inputType);
        }
        DBSPApplyOperator.noZsets(this.outputType());
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
           List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPApplyNOperator(
                    this.getRelNode(), toClosure(function), newInputs)
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
    public static DBSPApplyNOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPApplyNOperator(
                CalciteEmptyRel.INSTANCE, info.getClosureFunction(), info.inputs())
                .addAnnotations(info.annotations(), DBSPApplyNOperator.class);
    }
}
