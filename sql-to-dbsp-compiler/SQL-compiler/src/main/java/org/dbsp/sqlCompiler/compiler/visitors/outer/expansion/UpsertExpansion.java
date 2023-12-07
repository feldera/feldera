package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOutputOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertOperator;

public class UpsertExpansion extends OperatorExpansion {
    public final DBSPDelayOutputOperator delayOutput;
    public final DBSPUpsertOperator upsert;
    public final DBSPSumOperator sum;
    public final DBSPDelayOperator delay;

    public UpsertExpansion(
            DBSPDelayOutputOperator delayOutput,
            DBSPUpsertOperator upsert,
            DBSPSumOperator sum,
            DBSPDelayOperator delay) {
        this.delayOutput = delayOutput;
        this.upsert = upsert;
        this.delay = delay;
        this.sum = sum;
    }
}
