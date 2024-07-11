package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;

public interface CommonJoinExpansion {
    DBSPDelayedIntegralOperator getLeftIntegrator();
    DBSPDelayedIntegralOperator getRightIntegrator();
}
