package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum DeltaTableWriteMode {
    @JsonProperty("append")          Append,
    @JsonProperty("truncate")        Truncate,
    @JsonProperty("error_if_exists") ErrorIfExists,
}
