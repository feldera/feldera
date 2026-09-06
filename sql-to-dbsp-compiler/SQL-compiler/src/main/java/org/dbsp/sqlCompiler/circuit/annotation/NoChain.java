package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;

/** Do not use this operator in a ChainOperator.
 * Used for example by the operator following a {@link
 * org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator} - only
 * {@link org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator} works there. */
public class NoChain extends Annotation {
    public static NoChain fromJson(JsonNode unused) {
        return new NoChain();
    }
}
