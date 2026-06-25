package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum DynamoDBWriteMode {
    @JsonProperty("batch")         Batch,
    @JsonProperty("transactional") Transactional,
}
