package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum AvroEncoderKeyMode {
    @JsonProperty("none")       None,
    @JsonProperty("key_fields") KeyFields,
}
