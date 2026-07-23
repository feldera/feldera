package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum SnowflakeNumberMode {
    @JsonProperty("decimal") Decimal,
    @JsonProperty("double") Double,
}
