package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregator;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.Objects;

/*** Base class for operators that perform some form of aggregation. */
public abstract class DBSPAggregateOperatorBase extends DBSPUnaryOperator {
    // Initially 'aggregate' is not null, and 'function' is null.
    // After lowering these two are swapped.
    @Nullable
    public final DBSPAggregateList aggregate;

    protected DBSPAggregateOperatorBase(CalciteRelNode node, String operation,
                                        DBSPTypeIndexedZSet outputType,
                                        @Nullable DBSPAggregator function,
                                        @Nullable DBSPAggregateList aggregate,
                                        boolean multiset,
                                        OutputPort source,
                                        boolean containsIntegrate) {
        super(node, operation, function, outputType, multiset, source, containsIntegrate);
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

    @Override
    public void accept(InnerVisitor visitor) {
        if (this.aggregate != null) {
            visitor.property("aggregate");
            this.aggregate.accept(visitor);
        }
        super.accept(visitor);
    }

    public DBSPAggregateList getAggregate() {
        return Objects.requireNonNull(this.aggregate);
    }

    @Nullable
    public DBSPAggregator getAggregator() {
        if (this.function == null)
            return null;
        return this.function.to(DBSPAggregator.class);
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
