package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum JsonLines {
    @JsonProperty("multiple") Multiple,
    @JsonProperty("single")   Single,
}
