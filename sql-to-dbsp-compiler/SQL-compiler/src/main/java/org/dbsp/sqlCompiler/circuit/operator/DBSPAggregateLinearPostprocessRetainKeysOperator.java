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

public final class DBSPAggregateLinearPostprocessRetainKeysOperator extends DBSPBinaryOperator {
    public final DBSPClosureExpression postProcess;
    public final DBSPClosureExpression retainKeysFunction;

    // This operator is incremental-only
    public DBSPAggregateLinearPostprocessRetainKeysOperator(
            CalciteRelNode node,
            DBSPTypeIndexedZSet outputType,
            DBSPExpression function,
            DBSPClosureExpression postProcess,
            DBSPClosureExpression retainKeysFunction,
            OutputPort left, OutputPort right) {
        super(node, "aggregate_linear_postprocess_retain_keys",
                function, outputType, false, left, right);
        this.postProcess = postProcess;
        this.retainKeysFunction = retainKeysFunction;
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
        visitor.property("retainKeysFunction");
        this.retainKeysFunction.accept(visitor);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        throw new InternalCompilerError("Should not be called");
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPAggregateLinearPostprocessRetainKeysOperator(
                    this.getRelNode(), this.outputType.to(DBSPTypeIndexedZSet.class),
                    this.getFunction(), this.postProcess, this.retainKeysFunction,
                    newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPAggregateLinearPostprocessRetainKeysOperator agg =
                other.to(DBSPAggregateLinearPostprocessRetainKeysOperator.class);
        return this.getFunction().equivalent(agg.getFunction()) &&
                this.postProcess.equivalent(agg.postProcess) &&
                this.retainKeysFunction.equivalent(agg.retainKeysFunction);
    }

    @SuppressWarnings("unused")
    public static DBSPAggregateLinearPostprocessRetainKeysOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPClosureExpression postProcess = fromJsonInner(node, "postProcess", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression retainKeysFunction = fromJsonInner(node, "retainKeysFunction", decoder, DBSPClosureExpression.class);
        return new DBSPAggregateLinearPostprocessRetainKeysOperator(CalciteEmptyRel.INSTANCE, info.getIndexedZsetType(),
                info.getFunction(), postProcess, retainKeysFunction, info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPAggregateLinearPostprocessRetainKeysOperator.class);
    }
}
