package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

/** Configuration for the clock input connector. */
@SuppressWarnings("unused")
public class ClockConfig implements IValidateConfig {
    /** How often the clock ticks, for example {@code "1s"}. */
    @Nullable
    @JsonProperty("clock_resolution")
    public JsonNode clockResolution = null;

    /** Deprecated; use {@link #clockResolution}. */
    @Nullable
    @JsonProperty("clock_resolution_usecs")
    public JsonNode clockResolutionUsecs = null;

    @Nullable
    @JsonProperty("now_offset_ms")
    public Long nowOffsetMs = null;

    @JsonProperty("http_driven")
    public boolean httpDriven = false;

    @Override
    public boolean validate(ConfigReporter reporter) {
        ConfigDuration.Setting resolution = ConfigDuration.resolve(reporter,
                "clock_resolution", this.clockResolution,
                "clock_resolution_usecs", this.clockResolutionUsecs,
                ConfigDuration.NANOS_PER_MICRO);
        return ConfigDuration.checkMinimum(reporter, resolution, 1,
                "greater than 0, for example \"1s\"");
    }
}
