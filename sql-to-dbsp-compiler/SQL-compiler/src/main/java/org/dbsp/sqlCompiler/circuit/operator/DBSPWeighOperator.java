package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import java.util.List;

@NonCoreIR
public final class DBSPWeighOperator extends DBSPUnaryOperator {
    static DBSPTypeZSet outputType(DBSPTypeIndexedZSet sourceType) {
        return new DBSPTypeZSet(sourceType.elementType);
    }

    public DBSPWeighOperator(CalciteObject node, DBSPExpression function, OutputPort source) {
        super(node, "weigh", function,
                outputType(source.getOutputIndexedZSetType()), false, source);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWeighOperator(this.getNode(), this.getFunction(), newInputs.get(0))
                    .copyAnnotations(this);
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
    public static DBSPWeighOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPWeighOperator(CalciteObject.EMPTY, info.getFunction(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPWeighOperator.class);
    }
}
