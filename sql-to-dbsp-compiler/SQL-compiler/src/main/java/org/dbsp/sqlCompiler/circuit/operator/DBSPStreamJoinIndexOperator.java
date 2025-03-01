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

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Currently there is no corespondent operator in DBSP.
 * See {@link DBSPJoinIndexOperator} for the function signature. */
public final class DBSPStreamJoinIndexOperator extends DBSPJoinBaseOperator {
    public DBSPStreamJoinIndexOperator(
            CalciteRelNode node, DBSPTypeIndexedZSet outputType,
            DBSPExpression function, boolean isMultiset,
            OutputPort left, OutputPort right) {
        super(node, "stream_join_index", function, outputType, isMultiset, left, right);
        assert left.getOutputIndexedZSetType().keyType.sameType(right.getOutputIndexedZSetType().keyType);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPStreamJoinIndexOperator(
                this.getRelNode(), outputType.to(DBSPTypeIndexedZSet.class),
                Objects.requireNonNull(expression),
                this.isMultiset, this.left(), this.right()).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPStreamJoinIndexOperator(
                    this.getRelNode(), this.getOutputIndexedZSetType(),
                    this.getFunction(), this.isMultiset, newInputs.get(0), newInputs.get(1))
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

    @Override
    public DBSPJoinBaseOperator withFunctionAndInputs(DBSPExpression function, OutputPort left, OutputPort right) {
        return new DBSPStreamJoinIndexOperator(
                this.getRelNode(), this.getOutputIndexedZSetType(), function, this.isMultiset, left, right);
    }

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPStreamJoinIndexOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPExpression function = info.getFunction();
        return new DBSPStreamJoinIndexOperator(CalciteEmptyRel.INSTANCE,
                info.getIndexedZsetType(), function, info.isMultiset(), info.getInput(0), info.getInput(1));
    }
}
