package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Mirrors the Rust {@code JsonFlavor} enum.
 * The {@code ParquetConverter} variant is omitted: its {@code #[serde(skip)]} attribute
 * means serde will never serialize or deserialize it, so it cannot appear in JSON. */
public enum JsonFlavor {
    @JsonProperty("default")                      Default,
    @JsonProperty("debezium_mysql")               DebeziumMySql,
    @JsonProperty("debezium_postgres")            DebeziumPostgres,
    @JsonProperty("snowflake")                    Snowflake,
    @JsonProperty("kafka_connect_json_converter") KafkaConnectJsonConverter,
    @JsonProperty("pandas")                       Pandas,
    @JsonProperty("blockchain")                   Blockchain,
    @JsonProperty("c_hex")                        CHex,
    @JsonProperty("ClockInput")                   ClockInput,
    @JsonProperty("datagen")                      Datagen,
    @JsonProperty("postgres")                     Postgres,
}
