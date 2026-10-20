package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

/** Expansion of a {@link org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator}:
 * ΔL ⋈ z⁻¹R + z⁻¹L ⋈ ΔR + ΔL ⋈ ΔR. */
public final class JoinIndexDeltaExpansion
        extends OperatorDeltaExpansion
        implements CommonJoinDeltaExpansion {
    /** z⁻¹L, the delayed integral of ΔL */
    public final DBSPDelayedIntegralOperator leftIntegrator;
    /** z⁻¹R, the delayed integral of ΔR */
    public final DBSPDelayedIntegralOperator rightIntegrator;
    /** ΔL ⋈ z⁻¹R */
    public final DBSPStreamJoinIndexOperator leftDelta;
    /** z⁻¹L ⋈ ΔR */
    public final DBSPStreamJoinIndexOperator rightDelta;
    /** ΔL ⋈ ΔR */
    public final DBSPStreamJoinIndexOperator both;
    /** The sum of the three terms */
    public final DBSPSumOperator sum;

    public JoinIndexDeltaExpansion(DBSPDelayedIntegralOperator leftIntegrator,
                                   DBSPDelayedIntegralOperator rightIntegrator,
                                   DBSPStreamJoinIndexOperator leftDelta,
                                   DBSPStreamJoinIndexOperator rightDelta,
                                   DBSPStreamJoinIndexOperator both,
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
