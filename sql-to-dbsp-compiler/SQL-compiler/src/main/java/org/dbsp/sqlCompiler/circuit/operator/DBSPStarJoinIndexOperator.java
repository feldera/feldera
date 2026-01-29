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
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** A Join operator that has N inputs; each input is indexed and all keys must be of the same type;
 * the output is an IndexedZSet.  This operator is incremental-only. */
public class DBSPStarJoinIndexOperator extends DBSPStarJoinBaseOperator implements IIncremental {
    public DBSPStarJoinIndexOperator(CalciteRelNode node, DBSPType outputType, DBSPClosureExpression function,
                                     boolean isMultiset, List<OutputPort> inputs) {
        super(node, "inner_star_join_index", outputType, function, isMultiset, inputs);
        DBSPTypeIndexedZSet ix = outputType.to(DBSPTypeIndexedZSet.class);
        Utilities.enforce(function.getResultType().sameType(ix.getKVType()),
                () -> "Function result type " + function.getResultType() + "\nOperator result type" + ix.getKVType());
    }

    @Override
    public DBSPOperator with(@Nullable DBSPExpression function, DBSPType outputType, List<OutputPort> inputs, boolean force) {
        Utilities.enforce(function != null);
        if (this.mustReplace(force, function, inputs, outputType)) {
            return new DBSPStarJoinIndexOperator(this.getRelNode(), outputType, function.to(DBSPClosureExpression.class),
                    this.isMultiset, inputs).copyAnnotations(this);
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
    public static DBSPStarJoinIndexOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPStarJoinIndexOperator(
                CalciteEmptyRel.INSTANCE, info.getIndexedZsetType(), info.getClosureFunction(),
                info.isMultiset(), info.inputs())
                .addAnnotations(info.annotations(), DBSPStarJoinIndexOperator.class);
    }
}
