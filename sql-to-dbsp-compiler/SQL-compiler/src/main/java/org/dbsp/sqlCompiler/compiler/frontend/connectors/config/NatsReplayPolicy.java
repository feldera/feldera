package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Replay policy for a NATS JetStream consumer. */
@SuppressWarnings("unused")
public enum NatsReplayPolicy {
    @JsonProperty("Instant") Instant,
    @JsonProperty("Original") Original
}
