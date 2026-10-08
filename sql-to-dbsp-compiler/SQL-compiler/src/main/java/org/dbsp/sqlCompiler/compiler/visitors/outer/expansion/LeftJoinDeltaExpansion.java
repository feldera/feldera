package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

/** Expansion of a {@link org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator} or a
 * {@link org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator} into the change of its
 * output: ΔL ⋈ z⁻¹I(R) + z⁻¹I(L) ⋈ ΔR + ΔL ⋈ ΔR + pad(Δ(L ▷ R)). */
public class LeftJoinDeltaExpansion extends OperatorDeltaExpansion implements CommonJoinDeltaExpansion {
    /** z⁻¹I(L) */
    public final DBSPDelayedIntegralOperator leftIntegrator;
    /** z⁻¹I(R) */
    public final DBSPDelayedIntegralOperator rightIntegrator;
    /** ΔL ⋈ z⁻¹I(R) */
    public final DBSPJoinBaseOperator leftDelta;
    /** z⁻¹I(L) ⋈ ΔR */
    public final DBSPJoinBaseOperator rightDelta;
    /** ΔL ⋈ ΔR */
    public final DBSPJoinBaseOperator join;
    /** Δ(L ▷ R), from the incremental antijoin of ΔL and ΔR */
    public final DBSPAntiJoinOperator antiJoin;
    /** pad(Δ(L ▷ R)) */
    public final DBSPSimpleOperator map;
    /** The sum of the four terms */
    public final DBSPSumOperator sum;

    public LeftJoinDeltaExpansion(DBSPDelayedIntegralOperator leftIntegrator,
                                  DBSPDelayedIntegralOperator rightIntegrator,
                                  DBSPJoinBaseOperator leftDelta,
                                  DBSPJoinBaseOperator rightDelta,
                                  DBSPJoinBaseOperator join,
                                  DBSPAntiJoinOperator anti,
                                  DBSPSimpleOperator map,
                                  DBSPSumOperator sum) {
        this.leftIntegrator = leftIntegrator;
        this.rightDelta = rightDelta;
        this.rightIntegrator = rightIntegrator;
        this.leftDelta = leftDelta;
        this.join = join;
        this.sum = sum;
        this.antiJoin = anti;
        this.map = map;
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
