package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for the Delta Table input transport. */
@SuppressWarnings("unused")
public class DeltaTableReaderConfig implements IValidateConfig {
    @JsonProperty("uri")
    public String uri = "";

    /** Required. */
    @Nullable
    @JsonProperty("mode")
    public DeltaTableIngestMode mode = null;

    @JsonProperty("transaction_mode")
    public DeltaTableTransactionMode transactionMode = DeltaTableTransactionMode.None;

    @Nullable
    @JsonProperty("timestamp_column")
    public String timestampColumn = null;

    @Nullable
    @JsonProperty("filter")
    public String filter = null;

    @JsonProperty("skip_unused_columns")
    public boolean skipUnusedColumns = false;

    /** Only valid when {@code mode} is {@code snapshot} or {@code snapshot_and_follow}. */
    @Nullable
    @JsonProperty("snapshot_filter")
    public String snapshotFilter = null;

    @Nullable
    @JsonAlias("start_version")
    @JsonProperty("version")
    public Long version = null;

    @Nullable
    @JsonAlias("start_datetime")
    @JsonProperty("datetime")
    public String datetime = null;

    @Nullable
    @JsonProperty("end_version")
    public Long endVersion = null;

    /** Only valid in {@code cdc} mode. */
    @Nullable
    @JsonProperty("cdc_delete_filter")
    public String cdcDeleteFilter = null;

    /** Only valid in {@code cdc} mode. */
    @Nullable
    @JsonProperty("cdc_order_by")
    public String cdcOrderBy = null;

    @JsonProperty("num_parsers")
    public int numParsers = 4;

    @Nullable
    @JsonProperty("max_concurrent_readers")
    public Long maxConcurrentReaders = null;

    @JsonProperty("verbose")
    public int verbose = 0;

    @Nullable
    @JsonProperty("max_retries")
    public Long maxRetries = null;

    /** Absorbs flattened {@code object_store_config} keys so they are not rejected as unknown. */
    @JsonAnySetter
    public void setObjectStoreOption(String key, Object value) {}

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (mode == null) {
            reporter.warn("Invalid configuration", "required field \"mode\" is missing");
            return false;
        }
        if (version != null && datetime != null) {
            reporter.warnPath("version", "Invalid configuration",
                    "\"version\" and \"datetime\" are mutually exclusive");
            ok = false;
        }
        if (snapshotFilter != null
                && mode != DeltaTableIngestMode.Snapshot
                && mode != DeltaTableIngestMode.SnapshotAndFollow) {
            reporter.warnPath("snapshot_filter", "Invalid configuration",
                    "\"snapshot_filter\" is only valid with \"mode\": \"snapshot\" or \"snapshot_and_follow\"");
            ok = false;
        }
        if (cdcDeleteFilter != null && mode != DeltaTableIngestMode.Cdc) {
            reporter.warnPath("cdc_delete_filter", "Invalid configuration",
                    "\"cdc_delete_filter\" is only valid with \"mode\": \"cdc\"");
            ok = false;
        }
        if (cdcOrderBy != null && mode != DeltaTableIngestMode.Cdc) {
            reporter.warnPath("cdc_order_by", "Invalid configuration",
                    "\"cdc_order_by\" is only valid with \"mode\": \"cdc\"");
            ok = false;
        }
        ok = ok && this.checkNonEmpty(reporter, this.uri, "uri");
        return ok;
    }
}
