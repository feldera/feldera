package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;

public class DistinctExpansion extends OperatorExpansion {
    public final DBSPIntegrateOperator integrator;
    public final DBSPStreamDistinctOperator distinct;

    public DistinctExpansion(DBSPIntegrateOperator integrator,
                             DBSPStreamDistinctOperator distinct) {
        this.integrator = integrator;
        this.distinct = distinct;
    }
}
