package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;

/** Annotation set on a differentiation operator indicating
 * that it should be preserved by the incremental optimization step. */
public class NoInc extends Annotation {
    private NoInc() {}

    public static final NoInc INSTANCE = new NoInc();

    public static NoInc fromJson(JsonNode unused) {
        return INSTANCE;
    }
}
