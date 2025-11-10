package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

import javax.annotation.Nullable;

public class LeftJoinExpansion extends JoinExpansion {
    public LeftJoinExpansion(@Nullable DBSPDelayedIntegralOperator leftIntegrator,
                             @Nullable DBSPDelayedIntegralOperator rightIntegrator,
                             @Nullable DBSPStreamJoinOperator leftDelta,
                             @Nullable DBSPStreamJoinOperator rightDelta,
                             DBSPStreamJoinOperator both,
                             DBSPSumOperator sum) {
        super(leftIntegrator, rightIntegrator, leftDelta, rightDelta, both, sum);
    }
}
