package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

/** Batch processing configuration for the Postgres CDC input connector. */
@SuppressWarnings("unused")
public class PostgresCdcBatchConfig {
    @JsonProperty("max_fill_ms")
    public long maxFillMs = 10_000;

    @JsonProperty("memory_budget_ratio")
    public double memoryBudgetRatio = 0.2;

    /** Preferred size of one batch per active stream, for example {@code "32Mi"}. */
    @Nullable
    @JsonProperty("max_size")
    public JsonNode maxSize = null;

    /** Deprecated; use {@link #maxSize}. */
    @Nullable
    @JsonProperty("max_bytes")
    public JsonNode maxBytes = null;
}
