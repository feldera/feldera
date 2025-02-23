package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;

/** Annotation used on operators that are used for computing the waterline */
public class Waterline extends Annotation {
    public static final Waterline INSTANCE = new Waterline();

    private Waterline() {}

    public static Waterline fromJson(JsonNode unused) {
        return INSTANCE;
    }
}
