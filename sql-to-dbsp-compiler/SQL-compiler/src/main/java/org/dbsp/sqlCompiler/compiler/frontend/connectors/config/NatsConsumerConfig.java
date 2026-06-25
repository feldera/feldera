package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JetStream consumer configuration for a NATS input connector.
 * Fields typed as {@link JsonNode} are accepted as-is and validated more thoroughly at runtime.
 */
@SuppressWarnings("unused")
public class NatsConsumerConfig {
    @Nullable
    @JsonProperty("name")
    public String name = null;

    @Nullable
    @JsonProperty("description")
    public String description = null;

    @JsonProperty("filter_subjects")
    public List<String> filterSubjects = new ArrayList<>();

    @JsonProperty("replay_policy")
    public NatsReplayPolicy replayPolicy = NatsReplayPolicy.Instant;

    @JsonProperty("rate_limit")
    public long rateLimit = 0;

    /** Required. */
    @Nullable
    @JsonProperty("deliver_policy")
    public JsonNode deliverPolicy = null;

    @JsonProperty("max_waiting")
    public long maxWaiting = 0;

    @JsonProperty("metadata")
    public Map<String, String> metadata = new HashMap<>();

    @Nullable
    @JsonProperty("max_batch")
    public Long maxBatch = null;

    @Nullable
    @JsonProperty("max_bytes")
    public Long maxBytes = null;

    @Nullable
    @JsonProperty("max_expires")
    public JsonNode maxExpires = null;
}
