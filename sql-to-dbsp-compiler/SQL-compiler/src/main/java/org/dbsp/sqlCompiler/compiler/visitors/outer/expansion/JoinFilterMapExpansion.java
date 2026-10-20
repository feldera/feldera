package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;


/** Expansion of a {@link org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator}, a join fused with the filter σ that
 * follows it: σ(ΔL ⋈ z⁻¹R) + σ(z⁻¹L ⋈ ΔR) + σ(ΔL ⋈ ΔR). */
public final class JoinFilterMapExpansion
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
    /** Filter of {@code leftDelta} */
    public final DBSPFilterOperator leftFilter;
    /** Filter of {@code rightDelta} */
    public final DBSPFilterOperator rightFilter;
    /** Filter of {@code both} */
    public final DBSPFilterOperator filter;
    /** The sum of the three filters */
    public final DBSPSumOperator sum;

    public JoinFilterMapExpansion(DBSPDelayedIntegralOperator leftIntegrator,
                                  DBSPDelayedIntegralOperator rightIntegrator,
                                  DBSPStreamJoinOperator leftDelta,
                                  DBSPStreamJoinOperator rightDelta,
                                  DBSPStreamJoinOperator both,
                                  DBSPFilterOperator leftFilter,
                                  DBSPFilterOperator rightFilter,
                                  DBSPFilterOperator filter,
                                  DBSPSumOperator sum) {
        this.leftIntegrator = leftIntegrator;
        this.rightIntegrator = rightIntegrator;
        this.leftDelta = leftDelta;
        this.rightDelta = rightDelta;
        this.both = both;
        this.leftFilter = leftFilter;
        this.rightFilter = rightFilter;
        this.filter = filter;
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
