package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;

/** This operator only operates correctly on deltas.  To operate on collections it
 * must differentiate its input, and integrate its output. */
public final class DBSPPartitionedRollingAggregateOperator extends DBSPAggregateOperatorBase {
    public final DBSPExpression partitioningFunction;
    public final DBSPWindowBoundExpression lower;
    public final DBSPWindowBoundExpression upper;

    // TODO: support the linear version of this operator.
    public DBSPPartitionedRollingAggregateOperator(
            CalciteRelNode node,
            DBSPExpression partitioningFunction,
            // Initially 'function' is null, and the 'aggregate' is not.
            // After lowering 'aggregate' is not null, and 'function' has its expected shape
            @Nullable DBSPExpression function,
            @Nullable DBSPAggregate aggregate,
            DBSPWindowBoundExpression lower,
            DBSPWindowBoundExpression upper,
            // The output type of partitioned_rolling_aggregate cannot actually be represented using
            // the current IR, so this type is a lie.
            DBSPTypeIndexedZSet outputType,
            OutputPort input) {
        super(node, "partitioned_rolling_aggregate", outputType, function, aggregate, true, input);
        this.lower = lower;
        this.upper = upper;
        this.partitioningFunction = partitioningFunction;
        assert partitioningFunction.is(DBSPClosureExpression.class);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPPartitionedRollingAggregateOperator(
                this.getRelNode(), this.partitioningFunction,
                expression, this.aggregate, this.lower, this.upper,
                outputType.to(DBSPTypeIndexedZSet.class),
                this.input()).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPPartitionedRollingAggregateOperator(
                    this.getRelNode(), this.partitioningFunction, this.function, this.aggregate,
                    this.lower, this.upper, this.getOutputIndexedZSetType(),
                    newInputs.get(0)).copyAnnotations(this);
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPPartitionedRollingAggregateOperator otherOperator = other.as(DBSPPartitionedRollingAggregateOperator.class);
        if (otherOperator == null)
            return false;
        return this.partitioningFunction.equivalent(otherOperator.partitioningFunction) &&
                EquivalenceContext.equiv(this.aggregate, otherOperator.aggregate) &&
                EquivalenceContext.equiv(this.function, otherOperator.function) &&
                this.lower.equivalent(otherOperator.lower) &&
                this.upper.equivalent(otherOperator.upper);
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
        visitor.property("partitioningFunction");
        this.partitioningFunction.accept(visitor);
        if (this.aggregate != null) {
            visitor.property("aggregate");
            this.aggregate.accept(visitor);
        }
        visitor.property("lower");
        this.lower.accept(visitor);
        visitor.property("upper");
        this.upper.accept(visitor);
    }

    @SuppressWarnings("unused")
    public static DBSPPartitionedRollingAggregateOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPExpression partitioningFunction = fromJsonInner(node, "partitioningFunction", decoder, DBSPExpression.class);
        DBSPAggregate aggregate = null;
        if (node.has("aggregate"))
            aggregate = fromJsonInner(node, "aggregate", decoder, DBSPAggregate.class);
        DBSPWindowBoundExpression lower = fromJsonInner(node, "lower", decoder, DBSPWindowBoundExpression.class);
        DBSPWindowBoundExpression upper = fromJsonInner(node, "upper", decoder, DBSPWindowBoundExpression.class);
        return new DBSPPartitionedRollingAggregateOperator(
                CalciteEmptyRel.INSTANCE, partitioningFunction, info.function(),
                aggregate, lower, upper, info.getIndexedZsetType(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPPartitionedRollingAggregateOperator.class);
    }
}
