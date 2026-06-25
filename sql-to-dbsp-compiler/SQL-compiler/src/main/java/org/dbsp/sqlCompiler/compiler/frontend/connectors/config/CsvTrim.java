package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Whitespace trimming policy applied to CSV fields or header names. */
public enum CsvTrim {
    @JsonProperty("none")    None,
    @JsonProperty("headers") Headers,
    @JsonProperty("fields")  Fields,
    @JsonProperty("all")     All,
}
