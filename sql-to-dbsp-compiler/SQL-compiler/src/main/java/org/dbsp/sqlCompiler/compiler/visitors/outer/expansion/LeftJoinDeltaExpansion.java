package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

/** Expansion of a left join: ΔL ⋈ I(R) + I(L) ⋈ ΔR + ΔL ⋈ ΔR + pad(antijoin(ΔL, ΔR)), where the
 * incremental antijoin produces the change of the left rows without a match in R, and pad fills
 * their right columns with NULL.  The joins and pad produce indexed collections when the left join
 * does. */
public class LeftJoinDeltaExpansion extends OperatorDeltaExpansion implements CommonJoinDeltaExpansion {
    public final DBSPDelayedIntegralOperator leftIntegrator;
    public final DBSPDelayedIntegralOperator rightIntegrator;
    public final DBSPJoinBaseOperator leftDelta;
    public final DBSPJoinBaseOperator rightDelta;
    public final DBSPJoinBaseOperator join;
    public final DBSPAntiJoinOperator antiJoin;
    public final DBSPSimpleOperator map;
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
