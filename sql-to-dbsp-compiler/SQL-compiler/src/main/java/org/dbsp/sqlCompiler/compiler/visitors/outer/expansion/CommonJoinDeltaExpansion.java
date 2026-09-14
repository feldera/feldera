package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;


/** Part that is common to the expansion of all kinds of join operators */
public interface CommonJoinDeltaExpansion {
    DBSPDelayedIntegralOperator getLeftIntegrator();
    DBSPDelayedIntegralOperator getRightIntegrator();
}
