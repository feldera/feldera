package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;

public final class DistinctDeltaExpansion extends OperatorDeltaExpansion {
    public final DBSPIntegrateOperator integrator;
    public final DBSPBinaryDistinctOperator distinct;

    public DistinctDeltaExpansion(DBSPIntegrateOperator integrator,
                                  DBSPBinaryDistinctOperator distinct) {
        this.integrator = integrator;
        this.distinct = distinct;
    }
}
