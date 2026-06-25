package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum RawParserMode {
    @JsonProperty("blob")  Blob,
    @JsonProperty("lines") Lines,
}
