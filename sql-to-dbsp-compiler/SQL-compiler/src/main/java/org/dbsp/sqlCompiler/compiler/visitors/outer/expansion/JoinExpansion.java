package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

public class JoinExpansion extends OperatorExpansion {
    public final DBSPDelayedIntegralOperator leftIntegrator;
    public final DBSPDelayedIntegralOperator rightIntegrator;
    public final DBSPStreamJoinOperator leftDelta;
    public final DBSPStreamJoinOperator rightDelta;
    public final DBSPStreamJoinOperator both;
    public final DBSPSumOperator sum;

    public JoinExpansion(DBSPDelayedIntegralOperator leftIntegrator,
                         DBSPDelayedIntegralOperator rightIntegrator,
                         DBSPStreamJoinOperator leftDelta,
                         DBSPStreamJoinOperator rightDelta,
                         DBSPStreamJoinOperator both,
                         DBSPSumOperator sum) {
        this.leftIntegrator = leftIntegrator;
        this.rightIntegrator = rightIntegrator;
        this.leftDelta = leftDelta;
        this.rightDelta = rightDelta;
        this.both = both;
        this.sum = sum;
    }
}
