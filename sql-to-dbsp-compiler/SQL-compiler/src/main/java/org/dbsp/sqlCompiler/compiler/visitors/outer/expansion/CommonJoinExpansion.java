package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;

import javax.annotation.Nullable;

/** Part that is common to the expansion of all kinds of join operators */
public interface CommonJoinExpansion {
    @Nullable DBSPDelayedIntegralOperator getLeftIntegrator();
    @Nullable DBSPDelayedIntegralOperator getRightIntegrator();
}
