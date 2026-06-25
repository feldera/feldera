package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Configuration for the Avro output format.
 * Registry fields are inlined directly. */
@SuppressWarnings("unused")
public class AvroEncoderConfig implements IValidateConfig {
    @JsonProperty("update_format")
    public AvroUpdateFormat updateFormat = AvroUpdateFormat.Raw;

    @Nullable
    @JsonProperty("key_mode")
    public AvroEncoderKeyMode keyMode = null;

    @Nullable
    @JsonProperty("schema")
    public String schema = null;

    /** Only valid with {@code update_format = "raw"}. */
    @Nullable
    @JsonProperty("cdc_field")
    public String cdcField = null;

    @Nullable
    @JsonProperty("namespace")
    public String namespace = null;

    @Nullable
    @JsonProperty("subject_name_strategy")
    public SubjectNameStrategy subjectNameStrategy = null;

    @JsonProperty("skip_schema_id")
    public boolean skipSchemaId = false;

    @JsonProperty("threads")
    public int threads = 4;

    // ---- Schema registry fields (from AvroSchemaRegistryConfig) ----

    @JsonProperty("registry_urls")
    public List<String> registryUrls = new ArrayList<>();

    @JsonProperty("registry_headers")
    public Map<String, String> registryHeaders = new HashMap<>();

    @Nullable
    @JsonProperty("registry_proxy")
    public String registryProxy = null;

    @Nullable
    @JsonProperty("registry_timeout_secs")
    public Long registryTimeoutSecs = null;

    /** Mutually exclusive with {@code registry_authorization_token}. */
    @Nullable
    @JsonProperty("registry_username")
    public String registryUsername = null;

    @Nullable
    @JsonProperty("registry_password")
    public String registryPassword = null;

    /** Mutually exclusive with {@code registry_username} / {@code registry_password}. */
    @Nullable
    @JsonProperty("registry_authorization_token")
    public String registryAuthorizationToken = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (cdcField != null && updateFormat != AvroUpdateFormat.Raw) {
            reporter.warnPath("cdc_field", "Invalid configuration",
                    "\"cdc_field\" is only valid with \"update_format\": \"raw\"");
            ok = false;
        }
        if (registryAuthorizationToken != null
                && (registryUsername != null || registryPassword != null)) {
            reporter.warnPath("registry_authorization_token", "Invalid configuration",
                    "\"registry_authorization_token\" is mutually exclusive with "
                    + "\"registry_username\" and \"registry_password\"");
            ok = false;
        }
        return ok;
    }
}
