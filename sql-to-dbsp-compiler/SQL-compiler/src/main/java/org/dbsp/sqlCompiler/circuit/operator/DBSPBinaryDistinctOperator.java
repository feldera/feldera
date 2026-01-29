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

/** This operator does not exist in DBSP, it is purely used while computing monotonicity */
@NonCoreIR
public final class DBSPBinaryDistinctOperator extends DBSPBinaryOperator
        implements IContainsIntegrator, IIncremental {
    public DBSPBinaryDistinctOperator(CalciteRelNode node, OutputPort integral, OutputPort delta) {
        super(node, "distinct_component", null, delta.outputType(), false, integral, delta);
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
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            Utilities.enforce(newInputs.size() == 2);
            if (force || this.inputsDiffer(newInputs))
                return new DBSPBinaryDistinctOperator(
                        this.getRelNode(), newInputs.get(0), newInputs.get(1)).copyAnnotations(this);
        }
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPBinaryDistinctOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPBinaryDistinctOperator(CalciteEmptyRel.INSTANCE, info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPBinaryDistinctOperator.class);
    }
}
