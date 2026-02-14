package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ImplementJoins;

import java.util.List;

public class StarJoinDeltaExpansion extends OperatorDeltaExpansion {
    // The delta expansion of a star join ABC is DA B C + A DB C + A B DC
    public final List<DBSPDelayedIntegralOperator> integrators;
    public final List<ImplementJoins.StarJoinImplementation> joins;
    public final DBSPSumOperator sum;

    public StarJoinDeltaExpansion(List<DBSPDelayedIntegralOperator> integrators,
                                  List<ImplementJoins.StarJoinImplementation> joins,
                                  DBSPSumOperator sum) {
        this.integrators = integrators;
        this.joins = joins;
        this.sum = sum;
    }
}
