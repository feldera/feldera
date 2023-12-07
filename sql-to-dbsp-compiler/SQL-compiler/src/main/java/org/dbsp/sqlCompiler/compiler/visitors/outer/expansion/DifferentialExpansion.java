package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;

public class DifferentialExpansion extends OperatorExpansion {
    public final DBSPDelayOperator delay;
    public final DBSPSubtractOperator subtract;

    public DifferentialExpansion(DBSPDelayOperator delay,
                                 DBSPSubtractOperator subtract) {
        this.delay = delay;
        this.subtract = subtract;
    }
}
