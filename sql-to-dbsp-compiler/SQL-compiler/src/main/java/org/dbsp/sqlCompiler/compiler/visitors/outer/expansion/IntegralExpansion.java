package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOutputOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

public class IntegralExpansion extends OperatorExpansion {
    public final DBSPDelayOutputOperator delayOutput;
    public final DBSPSumOperator sum;
    public final DBSPDelayOperator delay;

    public IntegralExpansion(DBSPDelayOutputOperator delayOutput,
                             DBSPSumOperator sum,
                             DBSPDelayOperator delay) {
        this.delayOutput = delayOutput;
        this.sum = sum;
        this.delay = delay;
    }
}
