package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Options for connecting to a NATS server. */
@SuppressWarnings("unused")
public class NatsConnectOptionsConfig {
    /** NATS server URL (e.g., {@code "nats://localhost:4222"}). */
    @JsonProperty("server_url")
    public String serverUrl = "";

    @JsonProperty("auth")
    public NatsAuthConfig auth = new NatsAuthConfig();

    @JsonProperty("connection_timeout_secs")
    public long connectionTimeoutSecs = 10;

    @JsonProperty("request_timeout_secs")
    public long requestTimeoutSecs = 10;
}
