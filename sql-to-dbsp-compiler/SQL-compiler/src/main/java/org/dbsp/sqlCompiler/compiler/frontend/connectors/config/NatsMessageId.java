package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/** How {@code nats_output} chooses the {@code Nats-Msg-Id} header; mirrors the Rust enum. */
@SuppressWarnings("unused")
public enum NatsMessageId {
    @JsonProperty("content_hash") content_hash,
    @JsonProperty("none") none
}
