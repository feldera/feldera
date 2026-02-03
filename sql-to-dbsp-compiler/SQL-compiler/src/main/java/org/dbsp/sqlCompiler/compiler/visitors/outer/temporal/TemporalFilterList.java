package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A List of {@link TemporalFilter} objects that are "compatible" with each other.
 * These have the shape "(t.field > now() + constant)",
 * where the same field is used in all comparisons. */
record TemporalFilterList(List<TemporalFilter> comparisons) implements BooleanExpression {
    @Override
    public boolean compatible(BooleanExpression other) {
        return Linq.all(this.comparisons, c -> c.compatible(other));
    }

    @Override
    public BooleanExpression combine(BooleanExpression other) {
        this.comparisons.add(other.to(TemporalFilter.class));
        return this;
    }

    /** Combine two window bounds.  At least one must be non-null */
    static WindowBound combine(@Nullable WindowBound left, @Nullable WindowBound right, boolean lower) {
        if (left == null)
            return Objects.requireNonNull(right);
        if (right == null)
            return left;
        return left.combine(right, lower);
    }

    public WindowBounds getWindowBounds(DBSPCompiler compiler) {
        WindowBound lower = null;
        WindowBound upper = null;
        Utilities.enforce(!this.comparisons.isEmpty());
        DBSPExpression common = this.comparisons.get(0).noNow();
        for (TemporalFilter comp : this.comparisons) {
            boolean inclusive = RewriteNow.isInclusive(comp.opcode());
            boolean toLower = RewriteNow.isGreater(comp.opcode());
            WindowBound result = new WindowBound(inclusive, comp.withNow());
            if (toLower) {
                lower = combine(lower, result, toLower);
            } else {
                upper = combine(upper, result, toLower);
            }
        }
        return new WindowBounds(compiler, lower, upper, common);
    }

    @Override
    public BooleanExpression seal() {
        return this;
    }

    @Override
    public boolean isNullable() {
        return Linq.any(this.comparisons, BooleanExpression::isNullable);
    }

    public DBSPParameter getParameter() {
        return this.comparisons.get(0).parameter();
    }
}
