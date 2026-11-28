package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for writing data to an Iceberg table. */
@SuppressWarnings("unused")
public class IcebergWriterConfig extends IcebergCatalogConfig {
    /** Required. */
    @Nullable
    @JsonProperty("table_name")
    public String tableName = null;

    @Nullable
    @JsonProperty("mode")
    public IcebergWriteMode mode = null;

    @Nullable
    @JsonProperty("max_retries")
    public Long maxRetries = null;

    /** Must be {@code > 0} when set. */
    @Nullable
    @JsonProperty("threads")
    public Long threads = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (catalogType == null) {
            reporter.warn("Invalid configuration", "required field \"catalog_type\" is missing");
            return false;
        }
        if (tableName == null) {
            reporter.warn("Invalid configuration", "required field \"table_name\" is missing");
            ok = false;
        } else if (!tableName.matches("[^.]+(\\.[^.]+)+")) {
            reporter.warnPath("table_name", "Invalid configuration",
                    "\"table_name\" must have the format \"namespace.table\"");
            ok = false;
        }
        if (threads != null && threads == 0) {
            reporter.warnPath("threads", "Invalid configuration",
                    "\"threads\" must be greater than 0");
            ok = false;
        }
        ok &= validateCatalog(reporter);
        return ok;
    }
}
