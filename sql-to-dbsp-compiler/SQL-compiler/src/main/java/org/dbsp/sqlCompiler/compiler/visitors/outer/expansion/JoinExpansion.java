package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

public class JoinExpansion extends OperatorExpansion {
    public final DBSPDelayedIntegralOperator left;
    public final DBSPDelayedIntegralOperator right;
    public final DBSPStreamJoinOperator leftDelta;
    public final DBSPStreamJoinOperator rightDelta;
    public final DBSPStreamJoinOperator both;
    public final DBSPSumOperator sum;

    public JoinExpansion(DBSPDelayedIntegralOperator left,
                         DBSPDelayedIntegralOperator right,
                         DBSPStreamJoinOperator leftDelta,
                         DBSPStreamJoinOperator rightDelta,
                         DBSPStreamJoinOperator both,
                         DBSPSumOperator sum) {
        this.left = left;
        this.right = right;
        this.leftDelta = leftDelta;
        this.rightDelta = rightDelta;
        this.both = both;
        this.sum = sum;
    }
}
