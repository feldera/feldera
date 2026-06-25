package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum JsonUpdateFormat {
    @JsonProperty("insert_delete") InsertDelete,
    @JsonProperty("weighted")      Weighted,
    @JsonProperty("debezium")      Debezium,
    @JsonProperty("snowflake")     Snowflake,
    @JsonProperty("raw")           Raw,
    @JsonProperty("redis")         Redis,
}
