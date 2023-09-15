package org.dbsp.sqlCompiler.compiler.backend.jit;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Base class for various kinds of JIT outputs.
 * Corresponds to the Rust enum dataflow-jit/src/main.rs/OutputKind.
 */
public interface JitOutputDescription {
    /**
     * A Json representation of this output.
     * @param file File where output will be written.
     */
    JsonNode asJson(String file);
}
