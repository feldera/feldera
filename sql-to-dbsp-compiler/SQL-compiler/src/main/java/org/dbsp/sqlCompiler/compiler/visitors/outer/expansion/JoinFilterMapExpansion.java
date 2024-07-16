package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

public final class JoinFilterMapExpansion
        extends OperatorExpansion
        implements CommonJoinExpansion {
    public final DBSPDelayedIntegralOperator leftIntegrator;
    public final DBSPDelayedIntegralOperator rightIntegrator;
    public final DBSPStreamJoinOperator leftDelta;
    public final DBSPStreamJoinOperator rightDelta;
    public final DBSPStreamJoinOperator both;
    public final DBSPFilterOperator leftFilter;
    public final DBSPFilterOperator rightFilter;
    public final DBSPFilterOperator filter;
    public final DBSPSumOperator sum;

    public JoinFilterMapExpansion(DBSPDelayedIntegralOperator leftIntegrator,
                                  DBSPDelayedIntegralOperator rightIntegrator,
                                  DBSPStreamJoinOperator leftDelta,
                                  DBSPStreamJoinOperator rightDelta,
                                  DBSPStreamJoinOperator both,
                                  DBSPFilterOperator leftFilter,
                                  DBSPFilterOperator rightFilter,
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
    public DBSPDelayedIntegralOperator getLeftIntegrator() {
        return this.leftIntegrator;
    }

    @Override
    public DBSPDelayedIntegralOperator getRightIntegrator() {
        return this.rightIntegrator;
    }
}
