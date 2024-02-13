package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Base class for operators that perform some form of aggregation.
 */
public abstract class DBSPAggregateOperatorBase extends DBSPUnaryOperator {
    @Nullable
    public final DBSPAggregate aggregate;
    public final boolean isLinear;

    protected DBSPAggregateOperatorBase(CalciteObject node, String operation,
                                        DBSPTypeIndexedZSet outputType,
                                        @Nullable DBSPExpression function,
                                        @Nullable DBSPAggregate aggregate,
                                        boolean multiset,
                                        DBSPOperator source,
                                        boolean isLinear) {
        super(node, operation, function, outputType, multiset, source);
        this.aggregate = aggregate;
        this.isLinear = isLinear;
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
    public boolean hasFunction() {
        return true;
    }

    public DBSPAggregate getAggregate() {
        return Objects.requireNonNull(this.aggregate);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        this.writeComments(builder)
                .append("let ")
                .append(this.getOutputName())
                .append(": ")
                .append(this.outputStreamType)
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
