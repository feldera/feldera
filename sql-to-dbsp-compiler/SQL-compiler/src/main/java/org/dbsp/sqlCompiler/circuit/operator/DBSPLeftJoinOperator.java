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
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Corresponds to a DBSP left_join operator.
 * Output is always a ZSet. This operator is incremental-only (there is no "Stream" version). */
public final class DBSPLeftJoinOperator extends DBSPJoinBaseOperator {
    public DBSPLeftJoinOperator(
            CalciteRelNode node, DBSPTypeZSet outputType,
            DBSPExpression function, boolean isMultiset,
            OutputPort left, OutputPort right) {
        super(node, "left_join", function, outputType, isMultiset, left, right);
        this.checkResultType(function, this.getOutputZSetElementType());
        Utilities.enforce(left.getOutputIndexedZSetType().keyType.sameType(right.getOutputIndexedZSetType().keyType));
        Utilities.enforce(right.getOutputIndexedZSetType().elementType.mayBeNull);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType))
            return new DBSPLeftJoinOperator(
                this.getRelNode(), outputType.to(DBSPTypeZSet.class),
                Objects.requireNonNull(function),
                this.isMultiset, newInputs.get(0), newInputs.get(1)).copyAnnotations(this);
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

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPLeftJoinOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPLeftJoinOperator(
                CalciteEmptyRel.INSTANCE, info.getZsetType(), info.getFunction(),
                info.isMultiset(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPLeftJoinOperator.class);
    }
}
