package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Memory-based backpressure configuration for the Postgres CDC input connector. */
@SuppressWarnings("unused")
public class PostgresCdcMemoryBackpressureConfig {
    @JsonProperty("activate_threshold")
    public double activateThreshold = 0.85;

    @JsonProperty("resume_threshold")
    public double resumeThreshold = 0.75;
}
