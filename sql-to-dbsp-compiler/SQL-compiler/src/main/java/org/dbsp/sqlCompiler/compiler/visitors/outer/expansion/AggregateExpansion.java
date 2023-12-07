package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;

public class AggregateExpansion extends OperatorExpansion {
    public final DBSPIntegrateOperator integrator;
    public final DBSPPrimitiveAggregateOperator aggregator;
    public final DBSPUpsertFeedbackOperator upsert;

    public AggregateExpansion(DBSPIntegrateOperator integrator,
                              DBSPPrimitiveAggregateOperator aggregator,
                              DBSPUpsertFeedbackOperator upsert) {
        this.integrator = integrator;
        this.aggregator = aggregator;
        this.upsert = upsert;
    }
}
