package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum IcebergWriteMode {
    @JsonProperty("append")          Append,
    @JsonProperty("error_if_exists") ErrorIfExists,
}
