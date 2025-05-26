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
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** A join that produces an IndexedZSet.
 * Notice that, unlike the corresponding DBSP operator join_index,
 * this operator has a function that produces a single value, and not an Iterator.
 * The code generator has to wrap this value in a Some to be able to use the DBSP operator. */
public final class DBSPJoinIndexOperator extends DBSPJoinBaseOperator {
    public DBSPJoinIndexOperator(
            CalciteRelNode node, DBSPTypeIndexedZSet outputType,
            DBSPExpression function, boolean isMultiset,
            OutputPort left, OutputPort right) {
        super(node, "join_index", function, outputType, isMultiset, left, right);
        Utilities.enforce(left.getOutputIndexedZSetType().keyType.sameType(right.getOutputIndexedZSetType().keyType));
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType))
            return new DBSPJoinIndexOperator(
                this.getRelNode(), outputType.to(DBSPTypeIndexedZSet.class),
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
    public static DBSPJoinIndexOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPJoinIndexOperator(
                CalciteEmptyRel.INSTANCE, info.getIndexedZsetType(), info.getFunction(),
                info.isMultiset(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPJoinIndexOperator.class);
    }
}
