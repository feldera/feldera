package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

import javax.annotation.Nullable;

public final class JoinExpansion
        extends OperatorExpansion
        implements CommonJoinExpansion {
    @Nullable
    public final DBSPDelayedIntegralOperator leftIntegrator;
    @Nullable
    public final DBSPDelayedIntegralOperator rightIntegrator;
    @Nullable
    public final DBSPStreamJoinOperator leftDelta;
    @Nullable
    public final DBSPStreamJoinOperator rightDelta;
    public final DBSPStreamJoinOperator both;
    public final DBSPSumOperator sum;

    public JoinExpansion(@Nullable DBSPDelayedIntegralOperator leftIntegrator,
                         @Nullable DBSPDelayedIntegralOperator rightIntegrator,
                         @Nullable DBSPStreamJoinOperator leftDelta,
                         @Nullable DBSPStreamJoinOperator rightDelta,
                         DBSPStreamJoinOperator both,
                         DBSPSumOperator sum) {
        this.leftIntegrator = leftIntegrator;
        this.rightIntegrator = rightIntegrator;
        this.leftDelta = leftDelta;
        this.rightDelta = rightDelta;
        this.both = both;
        this.sum = sum;
    }

    @Override @Nullable
    public DBSPDelayedIntegralOperator getLeftIntegrator() {
        return this.leftIntegrator;
    }

    @Override @Nullable
    public DBSPDelayedIntegralOperator getRightIntegrator() {
        return this.rightIntegrator;
    }
}
