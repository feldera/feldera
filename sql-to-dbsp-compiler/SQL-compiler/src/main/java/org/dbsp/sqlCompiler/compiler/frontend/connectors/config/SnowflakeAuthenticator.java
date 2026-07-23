package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum SnowflakeAuthenticator {
    @JsonProperty("SNOWFLAKE_JWT") SnowflakeJwt,
}
