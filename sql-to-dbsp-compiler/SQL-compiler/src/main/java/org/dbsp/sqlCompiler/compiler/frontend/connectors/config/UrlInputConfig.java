package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

/** Configuration for reading data from an HTTP or HTTPS URL. */
@SuppressWarnings("unused")
public class UrlInputConfig implements IValidateConfig {
    @JsonProperty("path")
    public String path = "";

    /** Timeout before disconnection when paused, for example {@code "60s"}. */
    @Nullable
    @JsonProperty("pause_linger")
    public JsonNode pauseLinger = null;

    /** Deprecated; use {@link #pauseLinger}. */
    @JsonProperty("pause_timeout")
    @Nullable
    public JsonNode pauseTimeout = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        return ConfigDuration.check(reporter,
                "pause_linger", this.pauseLinger,
                "pause_timeout", this.pauseTimeout);
    }
}
