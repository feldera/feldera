package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum PostgresWriteMode {
    @JsonProperty("materialized") Materialized,
    @JsonProperty("cdc")          Cdc,
}
