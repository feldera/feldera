package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Anti join operator: produces elements from the left stream that have no corresponding keys
 * in the right stream. */
public final class DBSPAntiJoinOperator extends DBSPBinaryOperator {
    public DBSPAntiJoinOperator(CalciteRelNode node, OutputPort left, OutputPort right) {
        super(node, "antijoin", null, left.outputType(), left.isMultiset(), left, right, true);
        // Inputs must be indexed
        left.getOutputIndexedZSetType();
        right.getOutputIndexedZSetType();
        Utilities.enforce(left.getOutputIndexedZSetType()
                .keyType
                .sameType(right.getOutputIndexedZSetType()
                        .keyType),
                "Anti join key types to not match\n" +
                        left.getOutputIndexedZSetType().keyType + " and\n" +
                        right.getOutputIndexedZSetType().keyType);
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
            return new DBSPAntiJoinOperator(
                    this.getRelNode(), newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        }
        return this;
    }

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPAntiJoinOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPAntiJoinOperator(CalciteEmptyRel.INSTANCE, info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPAntiJoinOperator.class);
    }
}
