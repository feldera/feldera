package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;

/** Annotation used on {@link DBSPNestedOperator}s that contain recursive circuits */
public class Recursive extends Annotation {
    private Recursive() {}

    public static final Recursive INSTANCE = new Recursive();

    public static Recursive fromJson(JsonNode unused) {
        return INSTANCE;
    }
}
