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

public final class DBSPFlatMapIndexOperator extends DBSPUnaryOperator {
    public DBSPFlatMapIndexOperator(CalciteRelNode node, DBSPExpression expression,
                                    DBSPTypeIndexedZSet outputType, boolean isMultiset,
                                    OutputPort input) {
        super(node, "flat_map_index", expression, outputType, isMultiset, input);
        // We use this operator in a very restricted way,
        // to implement a MapIndex preceded or followed by a Filter.
        // So the iterator is always over an Option().
        checkArgumentFunctionType(expression, input);
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
            return new DBSPFlatMapIndexOperator(
                    this.getRelNode(), Objects.requireNonNull(function),
                    outputType.to(DBSPTypeIndexedZSet.class), this.isMultiset, newInputs.get(0))
                    .copyAnnotations(this);
        }
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPFlatMapIndexOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPFlatMapIndexOperator(CalciteEmptyRel.INSTANCE, info.getFunction(),
                info.getIndexedZsetType(), info.isMultiset(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPFlatMapIndexOperator.class);
    }
}
