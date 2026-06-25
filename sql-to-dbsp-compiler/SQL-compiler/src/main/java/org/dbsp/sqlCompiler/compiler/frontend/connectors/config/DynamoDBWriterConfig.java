package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for the DynamoDB output connector. */
@SuppressWarnings("unused")
public class DynamoDBWriterConfig implements IValidateConfig {
    private static final int BATCH_MAX = 25;
    private static final int TRANSACTIONAL_MAX = 100;

    @JsonProperty("table")
    public String table = "";

    @JsonProperty("region")
    public String region = "";

    @Nullable
    @JsonProperty("endpoint_url")
    public String endpointUrl = null;

    @Nullable
    @JsonProperty("aws_access_key_id")
    public String awsAccessKeyId = null;

    @Nullable
    @JsonProperty("aws_secret_access_key")
    public String awsSecretAccessKey = null;

    @Nullable
    @JsonProperty("batch_size")
    public Integer batchSize = null;

    @JsonProperty("write_mode")
    public DynamoDBWriteMode writeMode = DynamoDBWriteMode.Batch;

    @JsonProperty("max_buffer_size_bytes")
    public long maxBufferSizeBytes = 1024 * 1024;

    @JsonProperty("max_concurrent_requests")
    public int maxConcurrentRequests = 64;

    @JsonProperty("threads")
    public int threads = 1;

    @Nullable
    @JsonProperty("max_retries")
    public Integer maxRetries = 10;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (table.isBlank()) {
            reporter.warnPath("table", "Invalid configuration",
                    "\"table\" cannot be empty");
            ok = false;
        } else if (table.length() < 3 || table.length() > 255) {
            reporter.warnPath("table", "Invalid configuration",
                    "table name \"" + table + "\" is " + table.length()
                            + " character(s); DynamoDB requires 3–255 characters");
            ok = false;
        } else if (!table.chars().allMatch(
                c -> (Character.isLetterOrDigit(c) && c < 0x80) || c == '_' || c == '-' || c == '.')) {
            reporter.warnPath("table", "Invalid configuration",
                    "table name \"" + table + "\" contains invalid characters;"
                            + " DynamoDB requires [a-zA-Z0-9_.-]");
            ok = false;
        }
        if (region.isBlank()) {
            reporter.warnPath("region", "Invalid configuration",
                    "\"region\" cannot be empty");
            ok = false;
        }
        if (batchSize != null) {
            int maxBatchSize = writeMode == DynamoDBWriteMode.Batch ? BATCH_MAX : TRANSACTIONAL_MAX;
            if (batchSize == 0 || batchSize > maxBatchSize) {
                reporter.warnPath("batch_size", "Invalid configuration",
                        "\"batch_size\" must be between 1 and " + maxBatchSize
                                + " for \"" + (writeMode == DynamoDBWriteMode.Batch ? "batch" : "transactional")
                                + "\" write mode");
                ok = false;
            }
        }
        if (threads == 0) {
            reporter.warnPath("threads", "Invalid configuration",
                    "\"threads\" must be at least 1");
            ok = false;
        }
        if (maxBufferSizeBytes == 0) {
            reporter.warnPath("max_buffer_size_bytes", "Invalid configuration",
                    "\"max_buffer_size_bytes\" must be greater than 0");
            ok = false;
        }
        if (maxConcurrentRequests == 0) {
            reporter.warnPath("max_concurrent_requests", "Invalid configuration",
                    "\"max_concurrent_requests\" must be greater than 0");
            ok = false;
        }
        if ((awsAccessKeyId == null) != (awsSecretAccessKey == null)) {
            reporter.warn("Invalid configuration",
                    "\"aws_access_key_id\" and \"aws_secret_access_key\" must be specified together");
            ok = false;
        }
        return ok;
    }
}
