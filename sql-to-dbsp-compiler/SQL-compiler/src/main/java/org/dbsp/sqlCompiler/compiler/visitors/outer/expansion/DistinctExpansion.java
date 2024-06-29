package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctIncrementalOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;

public class DistinctExpansion extends OperatorExpansion {
    public final DBSPIntegrateOperator integrator;
    public final DBSPDistinctIncrementalOperator distinct;

    public DistinctExpansion(DBSPIntegrateOperator integrator,
                             DBSPDistinctIncrementalOperator distinct) {
        this.integrator = integrator;
        this.distinct = distinct;
    }
}
