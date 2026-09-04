package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Batch processing configuration for the Postgres CDC input connector. */
@SuppressWarnings("unused")
public class PostgresCdcBatchConfig {
    @JsonProperty("max_fill_ms")
    public long maxFillMs = 10_000;

    @JsonProperty("memory_budget_ratio")
    public double memoryBudgetRatio = 0.2;

    @JsonProperty("max_bytes")
    public long maxBytes = 8L * 1024 * 1024;
}
