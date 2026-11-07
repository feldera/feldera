package org.dbsp.sqlCompiler.circuit.operator.primitives;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IPrimitive;
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
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** The stateless step of a {@link DBSPChainAggregateOperator}.  Input 0 is the change of the
 * input in this step; input 1 is the output of the aggregate before this step.  For each key that
 * changes, the output removes the old value and inserts the value that {@link #init} and the step
 * function compute.  The function {@link #init} starts a group that has no value yet. */
public final class DBSPChainAggregateKernelOperator extends DBSPBinaryOperator implements IPrimitive {
    public final DBSPClosureExpression init;

    public DBSPChainAggregateKernelOperator(CalciteRelNode node, DBSPClosureExpression init,
                                            DBSPClosureExpression step, DBSPType outputType,
                                            OutputPort delta, OutputPort previous) {
        super(node, "chain_aggregate_kernel", step, outputType, delta, previous);
        this.init = init;
        Utilities.enforce(init.parameters.length == 2);
        Utilities.enforce(step.parameters.length == 3);
        Utilities.enforce(init.getResultType().sameType(step.getResultType()));
        Utilities.enforce(init.getResultType().sameType(step.parameters[0].getType()));
        Utilities.enforce(init.parameters[0].getType().sameType(step.parameters[1].getType()));
        Utilities.enforce(init.parameters[1].getType().sameType(DBSPTypeWeight.INSTANCE));
        Utilities.enforce(step.parameters[2].getType().sameType(DBSPTypeWeight.INSTANCE));
        Utilities.enforce(outputType.is(DBSPTypeIndexedZSet.class));
        Utilities.enforce(delta.outputType().is(DBSPTypeIndexedZSet.class));
        DBSPTypeIndexedZSet deltaType = delta.getOutputIndexedZSetType();
        DBSPTypeIndexedZSet resultType = this.getOutputIndexedZSetType();
        Utilities.enforce(deltaType.keyType.sameType(resultType.keyType));
        Utilities.enforce(deltaType.elementType.ref().sameType(init.parameters[0].getType()));
        this.checkResultType(step, resultType.elementType);
        Utilities.enforce(previous.outputType().sameType(outputType),
                () -> "Chain aggregate kernel " + this + " produces " + outputType +
                        " but reads the previous output as " + previous.outputType());
    }

    @Override
    protected StreamKind computeOutputKind() {
        this.requireInputAmong(0, StreamKind.DELTA_SNAPSHOT);
        this.requireInputAmong(1, StreamKind.COLLECTION_SNAPSHOT);
        return StreamKind.DELTA;
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPChainAggregateKernelOperator(this.getRelNode(), this.init,
                    Objects.requireNonNull(function).to(DBSPClosureExpression.class), outputType,
                    newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        }
        return this;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        super.accept(visitor);
        visitor.property("init");
        this.init.accept(visitor);
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
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPChainAggregateKernelOperator otherOperator = other.as(DBSPChainAggregateKernelOperator.class);
        return otherOperator != null && this.init.equivalent(otherOperator.init);
    }

    @SuppressWarnings("unused")
    public static DBSPChainAggregateKernelOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPClosureExpression init = fromJsonInner(node, "init", decoder, DBSPClosureExpression.class);
        return new DBSPChainAggregateKernelOperator(CalciteEmptyRel.INSTANCE, init,
                info.getClosureFunction(), info.outputType(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPChainAggregateKernelOperator.class);
    }
}
