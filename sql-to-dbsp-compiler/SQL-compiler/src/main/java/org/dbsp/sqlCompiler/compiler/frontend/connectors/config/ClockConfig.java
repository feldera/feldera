package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for the clock input connector. */
@SuppressWarnings("unused")
public class ClockConfig implements IValidateConfig {
    @JsonProperty("clock_resolution_usecs")
    public long clockResolutionUsecs = 0;

    @Nullable
    @JsonProperty("now_offset_ms")
    public Long nowOffsetMs = null;

    @JsonProperty("http_driven")
    public boolean httpDriven = false;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (clockResolutionUsecs <= 0) {
            reporter.warnPath("clock_resolution_usecs", "Invalid configuration",
                    "\"clock_resolution_usecs\" must be greater than 0");
            ok = false;
        }
        return ok;
    }
}
