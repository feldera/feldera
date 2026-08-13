package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for reading CDC data from Postgres logical replication. */
@SuppressWarnings("unused")
public class PostgresCdcReaderConfig implements IValidateConfig {
    @JsonProperty("uri")
    public String uri = "";

    @JsonProperty("publication")
    public String publication = "";

    @JsonProperty("source_table")
    public String sourceTable = "";

    // TLS fields (inlined from PostgresTlsConfig)
    @Nullable @JsonProperty("ssl_ca_pem")                    public String sslCaPem = null;
    @Nullable @JsonProperty("ssl_ca_location")               public String sslCaLocation = null;
    @Nullable @JsonProperty("ssl_client_pem")                public String sslClientPem = null;
    @Nullable @JsonProperty("ssl_client_location")           public String sslClientLocation = null;
    @Nullable @JsonProperty("ssl_client_key")                public String sslClientKey = null;
    @Nullable @JsonProperty("ssl_client_key_location")       public String sslClientKeyLocation = null;
    @Nullable @JsonProperty("ssl_certificate_chain_location") public String sslCertificateChainLocation = null;
    @Nullable @JsonProperty("verify_hostname")               public Boolean verifyHostname = null;

    @JsonProperty("batch")
    public PostgresCdcBatchConfig batch = new PostgresCdcBatchConfig();

    @JsonProperty("memory_backpressure")
    public PostgresCdcMemoryBackpressureConfig memoryBackpressure = new PostgresCdcMemoryBackpressureConfig();

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        ok = ok && this.checkNonEmpty(reporter, uri, "uri");
        ok = ok && this.checkNonEmpty(reporter, publication, "publication");
        ok = ok && this.checkNonEmpty(reporter, sourceTable, "source_table");

        if (batch.memoryBudgetRatio <= 0.0 || batch.memoryBudgetRatio > 1.0) {
            reporter.warnPath("batch/memory_budget_ratio", "Invalid configuration",
                    "\"batch.memory_budget_ratio\" must be in the (0.0, 1.0] interval");
            ok = false;
        }
        if (batch.maxBytes <= 0) {
            reporter.warnPath("batch/max_bytes", "Invalid configuration",
                    "\"batch.max_bytes\" must be greater than 0");
            ok = false;
        }

        if (memoryBackpressure.activateThreshold <= 0.0 || memoryBackpressure.activateThreshold > 1.0) {
            reporter.warnPath("memory_backpressure/activate_threshold", "Invalid configuration",
                    "\"memory_backpressure.activate_threshold\" must be in the (0.0, 1.0] interval");
            ok = false;
        }
        if (memoryBackpressure.resumeThreshold < 0.0 || memoryBackpressure.resumeThreshold >= 1.0) {
            reporter.warnPath("memory_backpressure/resume_threshold", "Invalid configuration",
                    "\"memory_backpressure.resume_threshold\" must be in the [0.0, 1.0) interval");
            ok = false;
        }
        if (memoryBackpressure.resumeThreshold >= memoryBackpressure.activateThreshold) {
            reporter.warnPath("memory_backpressure/resume_threshold", "Invalid configuration",
                    "\"memory_backpressure.resume_threshold\" must be lower than \"memory_backpressure.activate_threshold\"");
            ok = false;
        }
        return ok;
    }
}
