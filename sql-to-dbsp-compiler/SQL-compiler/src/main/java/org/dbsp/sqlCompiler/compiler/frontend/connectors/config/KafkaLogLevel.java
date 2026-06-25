package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum KafkaLogLevel {
    @JsonProperty("emerg")    Emerg,
    @JsonProperty("alert")    Alert,
    @JsonProperty("critical") Critical,
    @JsonProperty("error")    Error,
    @JsonProperty("warning")  Warning,
    @JsonProperty("notice")   Notice,
    @JsonProperty("info")     Info,
    @JsonProperty("debug")    Debug,
}
