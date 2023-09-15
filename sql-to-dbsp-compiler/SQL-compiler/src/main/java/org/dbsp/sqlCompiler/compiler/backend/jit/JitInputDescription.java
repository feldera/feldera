package org.dbsp.sqlCompiler.compiler.backend.jit;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Base class for various kinds of JIT inputs.
 * Corresponds to the Rust enum dataflow-jit/src/main.rs/InputKind.
 */
public interface JitInputDescription {
    /**
     * A Json representation of this input.
     * @param file File where input will be read from.
     */
    JsonNode asJson(String file);
}
