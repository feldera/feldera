package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

/** Configuration for the Google Pub/Sub input connector. */
@SuppressWarnings("unused")
public class PubSubInputConfig implements IValidateConfig {
    @Nullable
    @JsonProperty("emulator")
    public String emulator = null;

    @Nullable
    @JsonProperty("credentials")
    public String credentials = null;

    @Nullable
    @JsonProperty("endpoint")
    public String endpoint = null;

    @Nullable
    @JsonProperty("pool_size")
    public Integer poolSize = null;

    /** gRPC request timeout, for example {@code "30s"}. */
    @Nullable
    @JsonProperty("timeout")
    public JsonNode timeout = null;

    /** Deprecated; use {@link #timeout}. */
    @Nullable
    @JsonProperty("timeout_seconds")
    public JsonNode timeoutSeconds = null;

    /** gRPC connection timeout, for example {@code "10s"}. */
    @Nullable
    @JsonProperty("connect_timeout")
    public JsonNode connectTimeout = null;

    /** Deprecated; use {@link #connectTimeout}. */
    @Nullable
    @JsonProperty("connect_timeout_seconds")
    public JsonNode connectTimeoutSeconds = null;

    @Nullable
    @JsonProperty("project_id")
    public String projectId = null;

    @JsonProperty("subscription")
    public String subscription = "";

    @Nullable
    @JsonProperty("snapshot")
    public String snapshot = null;

    @Nullable
    @JsonProperty("timestamp")
    public String timestamp = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = ConfigDuration.check(reporter,
                "timeout", this.timeout,
                "timeout_seconds", this.timeoutSeconds);
        ok &= ConfigDuration.check(reporter,
                "connect_timeout", this.connectTimeout,
                "connect_timeout_seconds", this.connectTimeoutSeconds);
        if (snapshot != null && timestamp != null) {
            reporter.warnPath("snapshot", "Invalid configuration",
                    "\"snapshot\" and \"timestamp\" are mutually exclusive");
            ok = false;
        }
        return ok;
    }
}
