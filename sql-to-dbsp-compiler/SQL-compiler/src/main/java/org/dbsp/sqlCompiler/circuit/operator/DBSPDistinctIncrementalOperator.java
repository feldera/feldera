package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

@NonCoreIR
public final class DBSPDistinctIncrementalOperator extends DBSPBinaryOperator {
    public DBSPDistinctIncrementalOperator(CalciteRelNode node, OutputPort integral, OutputPort delta) {
        super(node, "distinct_incremental", null, delta.outputType(), false, integral, delta, false);
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
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return this;
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        Utilities.enforce(newInputs.size() == 2);
        if (force || this.inputsDiffer(newInputs))
            return new DBSPDistinctIncrementalOperator(
                    this.getRelNode(), newInputs.get(0), newInputs.get(1)).copyAnnotations(this);
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPDistinctIncrementalOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPDistinctIncrementalOperator(CalciteEmptyRel.INSTANCE, info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPDistinctIncrementalOperator.class);
    }
}
