package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;


/** Expansion of a {@link org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator}:
 * ΔL ⋈ z⁻¹R + z⁻¹L ⋈ ΔR + ΔL ⋈ ΔR. */
public final class JoinDeltaExpansion
        extends OperatorDeltaExpansion
        implements CommonJoinDeltaExpansion {
    /** z⁻¹L, the delayed integral of ΔL */
    public final DBSPDelayedIntegralOperator leftIntegrator;
    /** z⁻¹R, the delayed integral of ΔR */
    public final DBSPDelayedIntegralOperator rightIntegrator;
    /** ΔL ⋈ z⁻¹R */
    public final DBSPStreamJoinOperator leftDelta;
    /** z⁻¹L ⋈ ΔR */
    public final DBSPStreamJoinOperator rightDelta;
    /** ΔL ⋈ ΔR */
    public final DBSPStreamJoinOperator both;
    /** The sum of the three terms */
    public final DBSPSumOperator sum;

    public JoinDeltaExpansion(DBSPDelayedIntegralOperator leftIntegrator,
                              DBSPDelayedIntegralOperator rightIntegrator,
                              DBSPStreamJoinOperator leftDelta,
                              DBSPStreamJoinOperator rightDelta,
                              DBSPStreamJoinOperator both,
                              DBSPSumOperator sum) {
        this.leftIntegrator = leftIntegrator;
        this.rightIntegrator = rightIntegrator;
        this.leftDelta = leftDelta;
        this.rightDelta = rightDelta;
        this.both = both;
        this.sum = sum;
    }

    @Override
    public DBSPDelayedIntegralOperator getLeftIntegrator() {
        return this.leftIntegrator;
    }

    @Override
    public DBSPDelayedIntegralOperator getRightIntegrator() {
        return this.rightIntegrator;
    }
}
