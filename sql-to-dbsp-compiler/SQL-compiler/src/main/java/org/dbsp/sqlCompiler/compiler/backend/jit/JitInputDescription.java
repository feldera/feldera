package org.dbsp.sqlCompiler.compiler.backend.jit;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Base class for various kinds of JIT inputs.
 * Corresponds to the Rust enum dataflow-jit/src/main.rs/InputKind.
 */
public interface JitInputDescription {
    /**
     * A Json representation of this input, that is destined to be written to the specified file.
     */
    JsonNode asJson(String file);
}
