package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;

import javax.annotation.Nullable;

public class LeftJoinExpansion extends OperatorExpansion implements CommonJoinExpansion {
    public final DBSPDelayedIntegralOperator leftIntegrator;
    public final DBSPDelayedIntegralOperator rightIntegrator;
    public final DBSPStreamJoinOperator leftDelta;
    public final DBSPStreamJoinOperator rightDelta;
    public final DBSPStreamJoinOperator join;
    public final DBSPAntiJoinOperator antiJoin;
    public final DBSPMapOperator map;
    public final DBSPSumOperator sum;

    public LeftJoinExpansion(DBSPDelayedIntegralOperator leftIntegrator,
                             DBSPDelayedIntegralOperator rightIntegrator,
                             DBSPStreamJoinOperator leftDelta,
                             DBSPStreamJoinOperator rightDelta,
                             DBSPStreamJoinOperator join,
                             DBSPAntiJoinOperator anti,
                             DBSPMapOperator map,
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

    @Nullable
    @Override
    public DBSPDelayedIntegralOperator getLeftIntegrator() {
        return this.leftIntegrator;
    }

    @Nullable
    @Override
    public DBSPDelayedIntegralOperator getRightIntegrator() {
        return this.rightIntegrator;
    }
}
