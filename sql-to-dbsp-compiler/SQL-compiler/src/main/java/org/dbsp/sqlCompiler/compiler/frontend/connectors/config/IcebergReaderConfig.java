package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for reading data from an Iceberg table. */
@SuppressWarnings("unused")
public class IcebergReaderConfig extends IcebergCatalogConfig {
    /** Required. */
    @Nullable
    @JsonProperty("mode")
    public IcebergIngestMode mode = null;

    @Nullable
    @JsonProperty("timestamp_column")
    public String timestampColumn = null;

    @Nullable
    @JsonProperty("snapshot_filter")
    public String snapshotFilter = null;

    @Nullable
    @JsonProperty("snapshot_id")
    public Long snapshotId = null;

    @Nullable
    @JsonProperty("datetime")
    public String datetime = null;

    @Nullable
    @JsonProperty("metadata_location")
    public String metadataLocation = null;

    @Nullable
    @JsonProperty("table_name")
    public String tableName = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (mode == null) {
            reporter.warn("Invalid configuration", "required field \"mode\" is missing");
            return false;
        }
        if (snapshotId != null && datetime != null) {
            reporter.warnPath("snapshot_id", "Invalid configuration",
                    "\"snapshot_id\" and \"datetime\" are mutually exclusive");
            ok = false;
        }
        if (catalogType == null && metadataLocation == null) {
            reporter.warn("Invalid configuration",
                    "missing metadata location: specify an Iceberg catalog via \"catalog_type\""
                            + " or provide a table metadata location via \"metadata_location\"");
            ok = false;
        } else if (catalogType != null && metadataLocation != null) {
            reporter.warnPath("metadata_location", "Invalid configuration",
                    "\"metadata_location\" is not supported when \"catalog_type\" is set");
            ok = false;
        }
        if (catalogType == null && tableName != null) {
            reporter.warnPath("table_name", "Invalid configuration",
                    "\"table_name\" is only valid when \"catalog_type\" is set");
            ok = false;
        } else if (catalogType != null && tableName == null) {
            reporter.warn("Invalid configuration",
                    "\"table_name\" must be specified when \"catalog_type\" is set");
            ok = false;
        }
        ok &= validateCatalog(reporter);
        return ok;
    }
}
