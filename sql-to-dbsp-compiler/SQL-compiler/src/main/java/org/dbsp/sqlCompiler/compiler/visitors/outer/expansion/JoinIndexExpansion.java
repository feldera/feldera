package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

public final class JoinIndexExpansion
        extends OperatorExpansion
        implements CommonJoinExpansion {
    public final DBSPDelayedIntegralOperator leftIntegrator;
    public final DBSPDelayedIntegralOperator rightIntegrator;
    public final DBSPStreamJoinIndexOperator leftDelta;
    public final DBSPStreamJoinIndexOperator rightDelta;
    public final DBSPStreamJoinIndexOperator both;
    public final DBSPSumOperator sum;

    public JoinIndexExpansion(DBSPDelayedIntegralOperator leftIntegrator,
                              DBSPDelayedIntegralOperator rightIntegrator,
                              DBSPStreamJoinIndexOperator leftDelta,
                              DBSPStreamJoinIndexOperator rightDelta,
                              DBSPStreamJoinIndexOperator both,
                              DBSPSumOperator sum) {
        this.leftIntegrator = leftIntegrator;
        this.rightIntegrator = rightIntegrator;
        this.leftDelta = leftDelta;
        this.rightDelta = rightDelta;
        this.both = both;
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
