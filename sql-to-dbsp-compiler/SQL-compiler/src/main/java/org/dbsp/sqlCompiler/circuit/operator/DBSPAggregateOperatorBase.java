package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.Objects;

/*** Base class for operators that perform some form of aggregation. */
public abstract class DBSPAggregateOperatorBase extends DBSPUnaryOperator {
    // Initially 'aggregate' is not null, and 'function' is null.
    // After lowering these two are swapped.
    @Nullable
    public final DBSPAggregate aggregate;

    protected DBSPAggregateOperatorBase(CalciteRelNode node, String operation,
                                        DBSPTypeIndexedZSet outputType,
                                        @Nullable DBSPExpression function,
                                        @Nullable DBSPAggregate aggregate,
                                        boolean multiset,
                                        OutputPort source) {
        super(node, operation, function, outputType, multiset, source);
        this.aggregate = aggregate;
        // There are really two different representations of an aggregate operator,
        // which reuse the same classes: a high-level one, which contains an Aggregate,
        // and a low-level one, which contains a function.
        if (aggregate == null) {
            if (function == null)
                throw new InternalCompilerError("'function' and 'aggregate' are both null", node);
        } else {
            if (function != null)
                throw new InternalCompilerError("'function' and 'aggregate' are both non-null", node);
        }
    }

    public DBSPAggregate getAggregate() {
        return Objects.requireNonNull(this.aggregate);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPPartitionedRollingAggregateOperator otherOperator = other.as(DBSPPartitionedRollingAggregateOperator.class);
        if (otherOperator == null)
            return false;
        return EquivalenceContext.equiv(this.aggregate, otherOperator.aggregate);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        this.writeComments(builder)
                .append("let ")
                .append(this.getOutputName())
                .append(" = ")
                .append(this.input().getOutputName())
                .append(".")
                .append(this.operation)
                .append("(");
        if (this.function != null)
            builder.append(this.function);
        else if (this.aggregate != null)
            builder.append(this.aggregate);
        return builder.append(");");
    }
}
