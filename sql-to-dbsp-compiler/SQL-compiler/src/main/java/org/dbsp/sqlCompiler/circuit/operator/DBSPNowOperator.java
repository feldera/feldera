package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;

/** Operator that generates the NOW() timestamp.
 * There is no equivalent DBSP operator, this is only used during compilation to
 * represent an input which is connected to a stream containing clock values.
 * (The compiler option options.ioOptions.nowStream controls how this is implemented). */
public final class DBSPNowOperator extends DBSPSimpleOperator {
    static DBSPExpression createFunction(CalciteObject node) {
        return new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPApplyExpression(
                        "now", DBSPTypeTimestamp.create(node, false))));
    }

    public DBSPNowOperator(CalciteRelNode node) {
        super(node, "now", createFunction(node),
                new DBSPTypeZSet(new DBSPTypeTuple(DBSPTypeTimestamp.create(node, false))),
                false, false);
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
            @Nullable DBSPExpression expression, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPNowOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPNowOperator(CalciteEmptyRel.INSTANCE)
                .addAnnotations(info.annotations(), DBSPNowOperator.class);
    }
}
