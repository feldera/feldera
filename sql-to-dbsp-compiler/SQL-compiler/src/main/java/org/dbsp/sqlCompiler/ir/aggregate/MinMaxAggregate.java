package org.dbsp.sqlCompiler.ir.aggregate;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

/** Representation of an aggregate that is a call to a Min or Max function.
 * In some cases this will be treated as a NonLinearAggregate, in other cases
 * it is handled specially. */
public class MinMaxAggregate extends NonLinearAggregate {
    public final boolean isMin;
    /** An expression that refers to the row variable.  The row variable is
     * the second parameter of the 'increment' closure. */
    public final DBSPExpression aggregatedValue;

    public MinMaxAggregate(CalciteObject origin, DBSPExpression zero, DBSPClosureExpression increment,
                           DBSPExpression emptySetResult, DBSPType semigroup, DBSPExpression aggregatedValue,
                           boolean isMin) {
        super(origin, zero, increment, emptySetResult, semigroup);
        this.aggregatedValue = aggregatedValue;
        this.isMin = isMin;
    }

    @Override
    public boolean compatible(AggregateBase other) {
        return other.is(MinMaxAggregate.class);
    }
}
