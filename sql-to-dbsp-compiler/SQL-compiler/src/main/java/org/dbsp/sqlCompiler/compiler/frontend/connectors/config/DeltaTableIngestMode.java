package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum DeltaTableIngestMode {
    @JsonProperty("snapshot")           Snapshot,
    @JsonProperty("follow")             Follow,
    @JsonProperty("snapshot_and_follow") SnapshotAndFollow,
    @JsonProperty("cdc")                Cdc,
}
