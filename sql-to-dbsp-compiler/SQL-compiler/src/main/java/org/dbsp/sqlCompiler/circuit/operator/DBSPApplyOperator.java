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
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Equivalent to the apply operator from DBSP
 * which applies an arbitrary function to its input.
 * The inputs and outputs do not have to be Z-sets or indexed Z-sets.
 *
 * <p>Note: apply operators in DBSP behave differently: they are replicated in all workers,
 * and each worker performs the same computation on its local data.  The way we use apply operators
 * in the compiler, they are always fed directly or indirectly through a chain of apply operators
 * from a {@link DBSPWaterlineOperator}, which replicates its output to all workers.
 * So it's never OK to have an apply operator process inputs from standard operators.
 * In the type system such inputs would show up as ZSets or IndexedZSets. */
public final class DBSPApplyOperator extends DBSPUnaryOperator {
    public static void noZsets(DBSPType type) {
        assert !type.is(DBSPTypeZSet.class);
        assert !type.is(DBSPTypeIndexedZSet.class);
    }

    public DBSPApplyOperator(CalciteRelNode node, DBSPClosureExpression function,
                             DBSPType outputType, OutputPort input, @Nullable String comment) {
        super(node, "apply", function, outputType, false, input, comment, false);
        assert function.parameters.length == 1: "Expected 1 parameter for function " + function;
        DBSPType paramType = function.parameters[0].getType().deref();
        assert input.outputType().sameType(paramType) :
                "Parameter type " + paramType + " does not match input type " + input.outputType();
        noZsets(input.outputType());
        noZsets(this.outputType());
        assert function.getResultType().sameType(outputType) :
                "Function return type " + function.getResultType() + " does not match output type " + outputType;
    }

    public DBSPApplyOperator(CalciteRelNode node, DBSPClosureExpression function,
                             OutputPort input, @Nullable String comment) {
        this(node, function, function.getResultType(), input, comment);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPApplyOperator(
                this.getRelNode(), Objects.requireNonNull(expression).to(DBSPClosureExpression.class),
                outputType, this.input(), this.comment)
                .copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        assert newInputs.size() == 1: "Expected 1 input " + newInputs;
        if (force || this.inputsDiffer(newInputs)) {
            return new DBSPApplyOperator(
                    this.getRelNode(), this.getClosureFunction(),
                    newInputs.get(0), this.comment)
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
    public static DBSPApplyOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPApplyOperator(
                CalciteEmptyRel.INSTANCE, info.getClosureFunction(), info.outputType(), info.getInput(0), null)
                .addAnnotations(info.annotations(), DBSPApplyOperator.class);
    }
}
