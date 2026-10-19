package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveStarJoins;

import java.util.List;

/** Expansion of a {@link org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator} or a
 * {@link org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator} with inputs A₁ ... Aₙ:
 * Σᵢ z⁻¹A₁ ⋈ ... ⋈ ΔAᵢ ⋈ ... ⋈ z⁻¹Aₙ, one term per input.  The terms with several changes are
 * omitted: they are at least as monotone as the terms kept, so the monotonicity of the sum is the
 * same. */
public class StarJoinDeltaExpansion extends OperatorDeltaExpansion {
    /** z⁻¹A₁ ... z⁻¹Aₙ */
    public final List<DBSPDelayedIntegralOperator> integrators;
    /** The n terms, each a tree of binary joins */
    public final List<RemoveStarJoins.StarJoinImplementation> joins;
    /** The sum of the n terms */
    public final DBSPSumOperator sum;

    public StarJoinDeltaExpansion(List<DBSPDelayedIntegralOperator> integrators,
                                  List<RemoveStarJoins.StarJoinImplementation> joins,
                                  DBSPSumOperator sum) {
        this.integrators = integrators;
        this.joins = joins;
        this.sum = sum;
    }
}
