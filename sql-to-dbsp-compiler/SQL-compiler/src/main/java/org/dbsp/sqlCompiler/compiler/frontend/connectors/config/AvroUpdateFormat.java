package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum AvroUpdateFormat {
    @JsonProperty("raw")            Raw,
    @JsonProperty("debezium")       Debezium,
    @JsonProperty("confluent_jdbc") ConfluentJdbc,
}
