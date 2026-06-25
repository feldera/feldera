package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

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

    @Nullable
    @JsonProperty("timeout_seconds")
    public Integer timeoutSeconds = null;

    @Nullable
    @JsonProperty("connect_timeout_seconds")
    public Integer connectTimeoutSeconds = null;

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
        boolean ok = true;
        if (snapshot != null && timestamp != null) {
            reporter.warnPath("snapshot", "Invalid configuration",
                    "\"snapshot\" and \"timestamp\" are mutually exclusive");
            ok = false;
        }
        return ok;
    }
}
