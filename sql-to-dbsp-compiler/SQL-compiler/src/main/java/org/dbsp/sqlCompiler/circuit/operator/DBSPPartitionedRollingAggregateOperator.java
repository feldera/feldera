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
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregator;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** This operator only operates correctly on deltas.  To operate on collections it
 * must differentiate its input, and integrate its output. */
public final class DBSPPartitionedRollingAggregateOperator extends DBSPAggregateOperatorBase {
    public final DBSPClosureExpression partitioningFunction;
    public final DBSPWindowBoundExpression lower;
    public final DBSPWindowBoundExpression upper;

    // TODO: support the linear version of this operator.
    public DBSPPartitionedRollingAggregateOperator(
            CalciteRelNode node,
            DBSPClosureExpression partitioningFunction,
            // Initially 'function' is null, and the 'aggregate' is not.
            // After lowering 'aggregate' is not null, and 'function' has its expected shape
            @Nullable DBSPAggregator function,
            @Nullable DBSPAggregateList aggregate,
            DBSPWindowBoundExpression lower,
            DBSPWindowBoundExpression upper,
            // The output type of partitioned_rolling_aggregate cannot actually be represented using
            // the current IR, so this type is a lie.
            DBSPTypeIndexedZSet outputType,
            OutputPort input) {
        super(node, "partitioned_rolling_aggregate", outputType, function, aggregate, true, input, true);
        this.lower = lower;
        this.upper = upper;
        this.partitioningFunction = partitioningFunction;
        Utilities.enforce(partitioningFunction.is(DBSPClosureExpression.class));
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPPartitionedRollingAggregateOperator(
                    this.getRelNode(), this.partitioningFunction,
                    function != null ? function.to(DBSPAggregator.class) : null, this.aggregate,
                    this.lower, this.upper, outputType.to(DBSPTypeIndexedZSet.class),
                    newInputs.get(0)).copyAnnotations(this);
            }
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
        DBSPClosureExpression partitioningFunction = fromJsonInner(
                node, "partitioningFunction", decoder, DBSPClosureExpression.class);
        DBSPAggregateList aggregate = null;
        if (node.has("aggregate"))
            aggregate = fromJsonInner(node, "aggregate", decoder, DBSPAggregateList.class);
        DBSPWindowBoundExpression lower = fromJsonInner(node, "lower", decoder, DBSPWindowBoundExpression.class);
        DBSPWindowBoundExpression upper = fromJsonInner(node, "upper", decoder, DBSPWindowBoundExpression.class);
        return new DBSPPartitionedRollingAggregateOperator(
                CalciteEmptyRel.INSTANCE, partitioningFunction, (DBSPAggregator) info.function(),
                aggregate, lower, upper, info.getIndexedZsetType(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPPartitionedRollingAggregateOperator.class);
    }

    @Override
    public DBSPType outputStreamType(int outputNo, boolean outerCircuit) {
        Utilities.enforce(outputNo == 0);
        Utilities.enforce(outerCircuit);
        DBSPType[] args = new DBSPType[3];
        DBSPTypeRawTuple pfOut = this.partitioningFunction.getResultType().to(DBSPTypeRawTuple.class);
        args[0] = pfOut.tupFields[0];
        args[1] = this.lower.type;
        if (this.aggregate != null) {
            args[2] = this.aggregate.getType();
        } else {
            DBSPExpression expr = this.getFunction();
            args[2] = expr.getType().to(DBSPTypeFunction.class).resultType;
        }
        return new DBSPTypeUser(this.getRelNode(), DBSPTypeCode.USER,
                "OrdPartitionedOverStream", false, args);
    }
}
