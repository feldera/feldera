package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Username/password credentials for NATS authentication. */
public class NatsUserAndPasswordConfig {
    @JsonProperty("user")
    public String user = "";

    @JsonProperty("password")
    public String password = "";
}
