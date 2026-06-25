package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for reading data from AWS S3. */
@SuppressWarnings("unused")
public class S3InputConfig implements IValidateConfig {
    @Nullable
    @JsonProperty("aws_access_key_id")
    public String awsAccessKeyId = null;

    @Nullable
    @JsonProperty("aws_secret_access_key")
    public String awsSecretAccessKey = null;

    @JsonProperty("no_sign_request")
    public boolean noSignRequest = false;

    @Nullable
    @JsonProperty("key")
    public String key = null;

    @Nullable
    @JsonProperty("prefix")
    public String prefix = null;

    @JsonProperty("region")
    public String region = "";

    @JsonProperty("bucket_name")
    public String bucketName = "";

    @Nullable
    @JsonProperty("endpoint_url")
    public String endpointUrl = null;

    @JsonProperty("max_concurrent_fetches")
    public int maxConcurrentFetches = 8;

    @JsonProperty("max_retries")
    public int maxRetries = 5;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (region.isBlank()) {
            reporter.warnPath("region", "Invalid configuration",
                    "required field \"region\" is missing or empty");
            ok = false;
        }
        if (bucketName.isBlank()) {
            reporter.warnPath("bucket_name", "Invalid configuration",
                    "required field \"bucket_name\" is missing or empty");
            ok = false;
        }
        return ok;
    }
}
