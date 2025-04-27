package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

/** The z^-1 operator from DBSP.
 * If the function is specified, it is the initial value produced by the delay. */
public final class DBSPDelayOperator extends DBSPUnaryOperator {
    public DBSPDelayOperator(CalciteRelNode node, @Nullable DBSPExpression initial, OutputPort source) {
        super(node, initial == null ? "delay" : "delay_with_initial_value",
                initial, source.outputType(), source.isMultiset(), source);
        if (initial != null && !initial.getType().sameType(source.outputType()))
            throw new InternalCompilerError("Delay input has type " + source.outputType() +
                    " but initial value has type " + initial.getType());
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPDelayOperator(this.getRelNode(), this.function, newInputs.get(0))
                    .copyAnnotations(this);
        return this;
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression function, DBSPType unusedOutputType) {
        return new DBSPDelayOperator(this.getRelNode(), function, this.input())
                .copyAnnotations(this);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPDelayOperator otherOperator = other.as(DBSPDelayOperator.class);
        return otherOperator != null;
    }

    @SuppressWarnings("unused")
    public static DBSPDelayOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPDelayOperator(CalciteEmptyRel.INSTANCE, info.function(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPDelayOperator.class);
    }
}
