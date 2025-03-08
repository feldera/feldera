package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
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

public final class DBSPAggregateLinearPostprocessOperator extends DBSPUnaryOperator {
    public final DBSPClosureExpression postProcess;

    // This operator is incremental-only
    public DBSPAggregateLinearPostprocessOperator(
            CalciteRelNode node,
            DBSPTypeIndexedZSet outputType,
            DBSPExpression function,
            DBSPClosureExpression postProcess, OutputPort input) {
        super(node, "aggregate_linear_postprocess", function, outputType, false, input);
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
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        throw new InternalCompilerError("Should not be called");
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPAggregateLinearPostprocessOperator(
                    this.getRelNode(), this.outputType.to(DBSPTypeIndexedZSet.class),
                    this.getFunction(), this.postProcess, newInputs.get(0))
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
