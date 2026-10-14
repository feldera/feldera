package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;

import javax.annotation.Nullable;

/** Options for connecting to a NATS server. */
@SuppressWarnings("unused")
public class NatsConnectOptionsConfig {
    /** NATS server URL (e.g., {@code "nats://localhost:4222"}). */
    @JsonProperty("server_url")
    public String serverUrl = "";

    @JsonProperty("auth")
    public NatsAuthConfig auth = new NatsAuthConfig();

    /** Connection timeout, for example {@code "10s"}. */
    @Nullable
    @JsonProperty("connection_timeout")
    public JsonNode connectionTimeout = null;

    /** Deprecated; use {@link #connectionTimeout}. */
    @JsonProperty("connection_timeout_secs")
    @Nullable
    public JsonNode connectionTimeoutSecs = null;

    /** Request timeout, for example {@code "10s"}. */
    @Nullable
    @JsonProperty("request_timeout")
    public JsonNode requestTimeout = null;

    /** Deprecated; use {@link #requestTimeout}. */
    @JsonProperty("request_timeout_secs")
    @Nullable
    public JsonNode requestTimeoutSecs = null;

    /**
     * Validates the durations in this nested object.  {@code pathPrefix} is the JSON
     * Pointer suffix of this object within the connector config, so that errors point at
     * the key the user wrote.
     */
    public boolean validateDurations(ConfigReporter reporter, String pathPrefix) {
        boolean ok = ConfigDuration.check(reporter,
                pathPrefix + "/connection_timeout", this.connectionTimeout,
                pathPrefix + "/connection_timeout_secs", this.connectionTimeoutSecs);
        ok &= ConfigDuration.check(reporter,
                pathPrefix + "/request_timeout", this.requestTimeout,
                pathPrefix + "/request_timeout_secs", this.requestTimeoutSecs);
        return ok;
    }
}
