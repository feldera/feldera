package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

import javax.annotation.Nullable;

public final class JoinFilterMapExpansion
        extends OperatorExpansion
        implements CommonJoinExpansion {
    @Nullable public final DBSPDelayedIntegralOperator leftIntegrator;
    @Nullable public final DBSPDelayedIntegralOperator rightIntegrator;
    @Nullable public final DBSPStreamJoinOperator leftDelta;
    @Nullable public final DBSPStreamJoinOperator rightDelta;
    public final DBSPStreamJoinOperator both;
    @Nullable public final DBSPFilterOperator leftFilter;
    @Nullable public final DBSPFilterOperator rightFilter;
    public final DBSPFilterOperator filter;
    public final DBSPSumOperator sum;

    public JoinFilterMapExpansion(@Nullable DBSPDelayedIntegralOperator leftIntegrator,
                                  @Nullable DBSPDelayedIntegralOperator rightIntegrator,
                                  @Nullable DBSPStreamJoinOperator leftDelta,
                                  @Nullable DBSPStreamJoinOperator rightDelta,
                                  DBSPStreamJoinOperator both,
                                  @Nullable DBSPFilterOperator leftFilter,
                                  @Nullable DBSPFilterOperator rightFilter,
                                  DBSPFilterOperator filter,
                                  DBSPSumOperator sum) {
        this.leftIntegrator = leftIntegrator;
        this.rightIntegrator = rightIntegrator;
        this.leftDelta = leftDelta;
        this.rightDelta = rightDelta;
        this.both = both;
        this.leftFilter = leftFilter;
        this.rightFilter = rightFilter;
        this.filter = filter;
        this.sum = sum;
    }

    @Override
    @Nullable public DBSPDelayedIntegralOperator getLeftIntegrator() {
        return this.leftIntegrator;
    }

    @Override
    @Nullable public DBSPDelayedIntegralOperator getRightIntegrator() {
        return this.rightIntegrator;
    }
}
