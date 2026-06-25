package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum IcebergIngestMode {
    @JsonProperty("snapshot")             Snapshot,
    @JsonProperty("follow")               Follow,
    @JsonProperty("snapshot_and_follow")  SnapshotAndFollow,
}
