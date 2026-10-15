package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/** A header attached to every message published by {@code nats_output}. */
@SuppressWarnings("unused")
public class NatsHeaderConfig {
    @JsonProperty("key")
    public String key = "";

    @JsonProperty("value")
    public String value = "";
}
