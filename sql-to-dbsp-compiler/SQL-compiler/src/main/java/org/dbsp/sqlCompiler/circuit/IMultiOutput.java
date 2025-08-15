package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;

/** Interface implemented by operators with multiple outputs */
public interface IMultiOutput {
    int outputCount();
    OutputPort getOutput(int outputNo);
    /** This as an operator */
    DBSPOperator asOperator();
}
