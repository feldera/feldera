package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;

/** This annotation indicates that a DBSPOperator has an output that is always monotone.
 * This is intended to mark the part of the circuit that derives from the NOW table. */
public class AlwaysMonotone extends Annotation {
    private AlwaysMonotone() {}

    public static final AlwaysMonotone INSTANCE = new AlwaysMonotone();

    public static AlwaysMonotone fromJson(JsonNode unused) {
        return INSTANCE;
    }
}
