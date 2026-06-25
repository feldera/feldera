package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum DeltaTableTransactionMode {
    @JsonProperty("none")     None,
    @JsonProperty("snapshot") Snapshot,
    @JsonProperty("always")   Always,
    @JsonProperty("catchup")  Catchup,
}
