package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeighOperator;

public class StreamAggregateExpansion extends OperatorExpansion {
    public final DBSPWeighOperator weigh;
    public final DBSPStreamAggregateOperator aggregate;

    public StreamAggregateExpansion(DBSPWeighOperator weigh, DBSPStreamAggregateOperator aggregate) {
        this.weigh = weigh;
        this.aggregate = aggregate;
    }
}
