package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum IcebergCatalogType {
    @JsonProperty("rest")  Rest,
    @JsonProperty("glue")  Glue,
}
