package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

/** Configuration for the Postgres output connector. */
@SuppressWarnings("unused")
public class PostgresWriterConfig implements IValidateConfig {
    private static final String DEFAULT_CDC_OP_COLUMN = "__feldera_op";
    private static final String DEFAULT_CDC_TS_COLUMN = "__feldera_ts";

    @JsonProperty("uri")
    public String uri = "";

    @JsonProperty("table")
    public String table = "";

    @JsonProperty("mode")
    public PostgresWriteMode mode = PostgresWriteMode.Materialized;

    @JsonProperty("cdc_op_column")
    public String cdcOpColumn = DEFAULT_CDC_OP_COLUMN;

    @JsonProperty("cdc_ts_column")
    public String cdcTsColumn = DEFAULT_CDC_TS_COLUMN;

    // TLS fields (inlined from PostgresTlsConfig)
    @Nullable @JsonProperty("ssl_ca_pem")                    public String sslCaPem = null;
    @Nullable @JsonProperty("ssl_ca_location")               public String sslCaLocation = null;
    @Nullable @JsonProperty("ssl_client_pem")                public String sslClientPem = null;
    @Nullable @JsonProperty("ssl_client_location")           public String sslClientLocation = null;
    @Nullable @JsonProperty("ssl_client_key")                public String sslClientKey = null;
    @Nullable @JsonProperty("ssl_client_key_location")       public String sslClientKeyLocation = null;
    @Nullable @JsonProperty("ssl_certificate_chain_location") public String sslCertificateChainLocation = null;
    @Nullable @JsonProperty("verify_hostname")               public Boolean verifyHostname = null;

    @Nullable
    @JsonProperty("max_records_in_buffer")
    public Long maxRecordsInBuffer = null;

    @JsonProperty("max_buffer_size_bytes")
    public long maxBufferSizeBytes = 1 << 20;

    @JsonProperty("on_conflict_do_nothing")
    public boolean onConflictDoNothing = false;

    @JsonProperty("threads")
    public int threads = 1;

    @Nullable
    @JsonProperty("extra_columns")
    public List<String> extraColumns = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        switch (mode) {
            case Cdc -> {
                if (cdcOpColumn.isBlank()) {
                    reporter.warnPath("cdc_op_column", "Invalid configuration",
                            "\"cdc_op_column\" cannot be empty in CDC mode");
                    ok = false;
                } else if (!cdcOpColumn.chars().allMatch(c -> c < 128)) {
                    reporter.warnPath("cdc_op_column", "Invalid configuration",
                            "\"cdc_op_column\" must contain only ASCII characters");
                    ok = false;
                }
                if (cdcTsColumn.isBlank()) {
                    reporter.warnPath("cdc_ts_column", "Invalid configuration",
                            "\"cdc_ts_column\" cannot be empty in CDC mode");
                    ok = false;
                } else if (!cdcTsColumn.chars().allMatch(c -> c < 128)) {
                    reporter.warnPath("cdc_ts_column", "Invalid configuration",
                            "\"cdc_ts_column\" must contain only ASCII characters");
                    ok = false;
                }
                if (onConflictDoNothing) {
                    reporter.warnPath("on_conflict_do_nothing", "Invalid configuration",
                            "\"on_conflict_do_nothing\" is not supported in CDC mode");
                    ok = false;
                }
            }
            case Materialized -> {
                if (!cdcTsColumn.equals(DEFAULT_CDC_TS_COLUMN) && !cdcTsColumn.isBlank()) {
                    reporter.warnPath("cdc_ts_column", "Invalid configuration",
                            "\"cdc_ts_column\" must not be set in MATERIALIZED mode");
                    ok = false;
                }
                if (!cdcOpColumn.equals(DEFAULT_CDC_OP_COLUMN) && !cdcOpColumn.isBlank()) {
                    reporter.warnPath("cdc_op_column", "Invalid configuration",
                            "\"cdc_op_column\" must not be set in MATERIALIZED mode");
                    ok = false;
                }
            }
        }
        if (threads == 0) {
            reporter.warnPath("threads", "Invalid configuration",
                    "\"threads\" must be at least 1");
            ok = false;
        }
        return ok;
    }
}
