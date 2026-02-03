package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.util.ICastable;

/** Boolean expression that appear as a conjunct in a filter expression */
public interface BooleanExpression extends ICastable {
    /** true if these two Boolean expressions can be evaluated together */
    boolean compatible(BooleanExpression other);

    /** Combine two compatible boolean expressions */
    BooleanExpression combine(BooleanExpression other);

    /** Returns the final form of this Boolean expression */
    BooleanExpression seal();

    boolean isNullable();
}
