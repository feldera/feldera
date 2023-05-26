package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Base class for operators that perform some form of aggregation.
 */
public abstract class DBSPAggregateOperatorBase extends DBSPUnaryOperator {
    @Nullable
    public final DBSPAggregate aggregate;

    protected DBSPAggregateOperatorBase(@Nullable Object node, String operation, DBSPType outputType,
                                        @Nullable DBSPExpression function,
                                        @Nullable DBSPAggregate aggregate,
                                        boolean multiset,
                                        DBSPOperator source) {
        super(node, operation, function, outputType, multiset, source);
        this.aggregate = aggregate;
        // There are really two different representations of an aggregate operator,
        // which reuse the same classes: a high-level one, which contains an Aggregate,
        // and a low-level one, which contains a function.
        if (aggregate == null) {
            if (function == null)
                throw new RuntimeException("'function' and 'aggregate' are both null");
        } else {
            if (function != null)
                throw new RuntimeException("'function' and 'aggregate' are both non-null");
        }
    }

    public DBSPAggregate getAggregate() {
        return Objects.requireNonNull(this.aggregate);
    }
}
