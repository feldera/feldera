package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

/** Expansion of a left join fused with the filter that follows it: the terms of a
 * {@link LeftJoinDeltaExpansion}, each followed by the filter. */
public final class LeftJoinFilterMapExpansion extends LeftJoinDeltaExpansion {
    /** Filter of {@code leftDelta} */
    public final DBSPFilterOperator leftFilter;
    /** Filter of {@code rightDelta} */
    public final DBSPFilterOperator rightFilter;
    /** Filter of {@code join} */
    public final DBSPFilterOperator filter;
    /** Filter of {@code map} */
    public final DBSPFilterOperator mapFilter;

    public LeftJoinFilterMapExpansion(DBSPDelayedIntegralOperator leftIntegrator,
                                      DBSPDelayedIntegralOperator rightIntegrator,
                                      DBSPJoinBaseOperator leftDelta,
                                      DBSPJoinBaseOperator rightDelta,
                                      DBSPJoinBaseOperator join,
                                      DBSPAntiJoinOperator anti,
                                      DBSPSimpleOperator map,
                                      DBSPFilterOperator leftFilter,
                                      DBSPFilterOperator rightFilter,
                                      DBSPFilterOperator filter,
                                      DBSPFilterOperator mapFilter,
                                      DBSPSumOperator sum) {
        super(leftIntegrator, rightIntegrator, leftDelta, rightDelta, join, anti, map, sum);
        this.leftFilter = leftFilter;
        this.rightFilter = rightFilter;
        this.filter = filter;
        this.mapFilter = mapFilter;
    }
}
