package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public final class DBSPAggregateLinearPostprocessOperator extends DBSPUnaryOperator {
    public final DBSPClosureExpression postProcess;

    // This operator is incremental-only
    public DBSPAggregateLinearPostprocessOperator(
            CalciteRelNode node,
            DBSPTypeIndexedZSet outputType,
            DBSPExpression function,
            DBSPClosureExpression postProcess, OutputPort input) {
        super(node, "aggregate_linear_postprocess", function, outputType, false, input, true);
        this.postProcess = postProcess;
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
    public void accept(InnerVisitor visitor) {
        super.accept(visitor);
        visitor.property("postProcess");
        this.postProcess.accept(visitor);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType, List<OutputPort> inputs, boolean force) {
        if (this.mustReplace(force, function, inputs, outputType))
            return new DBSPAggregateLinearPostprocessOperator(
                    this.getRelNode(), outputType.to(DBSPTypeIndexedZSet.class),
                    Objects.requireNonNull(function), this.postProcess, inputs.get(0))
                    .copyAnnotations(this);
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPAggregateLinearPostprocessOperator agg = other.to(DBSPAggregateLinearPostprocessOperator.class);
        return this.getFunction().equivalent(agg.getFunction()) &&
                this.postProcess.equivalent(agg.postProcess);
    }

    @SuppressWarnings("unused")
    public static DBSPAggregateLinearPostprocessOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPClosureExpression postProcess = fromJsonInner(node, "postProcess", decoder, DBSPClosureExpression.class);
        return new DBSPAggregateLinearPostprocessOperator(CalciteEmptyRel.INSTANCE, info.getIndexedZsetType(),
                info.getFunction(), postProcess, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPAggregateLinearPostprocessOperator.class);
    }
}
